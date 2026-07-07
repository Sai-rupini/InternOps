const cron = require('node-cron');
const fs = require('fs').promises;
const path = require('path');
const pool = require('../config/db');
const pLimit = require('p-limit');

let cleanupRunning = false;
let reminderRunning = false;

const CONCURRENCY = 20;
const BATCH_SIZE = 500;

const emailService = require('../services/email');

async function runDeadlineReminderJob() {
  const jobName = 'deadline-reminder';
  const startTime = Date.now();

  console.info(
    JSON.stringify({
      job: jobName,
      startedAt: new Date(startTime),
    }),
    'Cron job started'
  );

  try {
    let offset = 0;
    let remindersSent = 0;
    const sentTaskIds = new Set();

    while (true) {
      const { rows: pendingTasks } = await pool.query(
        `
        SELECT DISTINCT
          st.id AS task_id,
          st.title AS task_title,
          st.deadline,
          u.id AS intern_id,
          u.email,
          u.full_name
        FROM social_tasks st
        JOIN task_assignments ta ON ta.task_id = st.id AND ta.deleted_at IS NULL
        JOIN users u ON u.id = ta.user_id AND u.role IN ('INTERN', 'CAPTAIN')
        WHERE st.deadline BETWEEN NOW() AND NOW() + INTERVAL '6 hours'
          AND st.deleted_at IS NULL
          AND st.reminder_sent_at IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM proof_submissions ps
            WHERE ps.task_id = st.id AND ps.intern_id = u.id AND ps.deleted_at IS NULL
          )
        ORDER BY st.id ASC
        LIMIT $1 OFFSET $2
        `,
        [BATCH_SIZE, offset]
      );

      if (pendingTasks.length === 0) break;

      for (const row of pendingTasks) {
        const hoursUntilDeadline = Math.round(
          (new Date(row.deadline) - new Date()) / (1000 * 60 * 60)
        );

        try {
          await emailService.sendNotification(row.email, {
            title: 'Deadline Reminder',
            message: `Hi ${row.full_name || 'there'}, the task "${row.task_title}" is due in ${hoursUntilDeadline <= 1 ? 'less than 1 hour' : hoursUntilDeadline + ' hours'}.`,
            actionUrl: `${process.env.APP_URL || 'http://localhost:5173'}/tasks`,
            actionText: 'Submit Proof',
          });
          remindersSent++;
          sentTaskIds.add(row.task_id);
        } catch (err) {
          console.error(
            `Failed to send reminder to ${row.email}: ${err.message}`
          );
        }
      }

      offset += BATCH_SIZE;

      if (pendingTasks.length < BATCH_SIZE) break;
    }

    if (sentTaskIds.size > 0) {
      await pool.query(
        `UPDATE social_tasks SET reminder_sent_at = NOW() WHERE id = ANY($1::uuid[])`,
        [Array.from(sentTaskIds)]
      );
    }

    console.info(
      JSON.stringify({
        job: jobName,
        durationMs: Date.now() - startTime,
        tasksWithPendingSubmissions: sentTaskIds.size,
        remindersSent,
      }),
      'Cron job completed'
    );

    return { tasksWithPendingSubmissions: sentTaskIds.size, remindersSent };
  } catch (err) {
    console.error(
      JSON.stringify({
        job: jobName,
        err: err.message,
        stack: err.stack,
      }),
      'Cron job failed'
    );
    throw err;
  }
}

async function runCleanupJob() {
  const jobName = 'proof-image-cleanup';
  const startTime = Date.now();

  console.info(
    JSON.stringify({
      job: jobName,
      startedAt: new Date(startTime),
    }),
    'Cron job started'
  );

  try {
    const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const uploadsRoot = path.resolve(__dirname, '..', '..', 'uploads');

    let totalProcessed = 0;
    let filesDeleted = 0;
    let totalUpdated = 0;
    let offset = 0;

    while (true) {
      const { rows } = await pool.query(
        `
        SELECT id, image_path
        FROM proof_submissions
        WHERE status = 'VERIFIED'
          AND verified_at < $1
          AND image_path IS NOT NULL
        ORDER BY id
        LIMIT $2 OFFSET $3
        `,
        [cutoff, BATCH_SIZE, offset]
      );

      if (rows.length === 0) break;

      totalProcessed += rows.length;

      const deletedIds = [];
      const limit = pLimit(CONCURRENCY);

      const results = await Promise.allSettled(
        rows.map((row) =>
          limit(async () => {
            const filePath = path.resolve(
              __dirname,
              '..',
              '..',
              row.image_path
            );

            const relative = path.relative(uploadsRoot, filePath);

            if (relative.startsWith('..') || path.isAbsolute(relative)) {
              console.error(
                `Invalid path for record ${row.id}: ${row.image_path}`
              );
              return;
            }

            try {
              await fs.unlink(filePath);
              filesDeleted++;
            } catch (err) {
              if (err.code !== 'ENOENT') {
                console.error(
                  `Failed deleting ${row.image_path}: ${err.message}`
                );
                return;
              }
            }

            deletedIds.push(row.id);
          })
        )
      );

      results.forEach((result) => {
        if (result.status === 'rejected') {
          console.error(result.reason);
        }
      });

      if (deletedIds.length > 0) {
        await pool.query(
          `
          UPDATE proof_submissions
          SET image_path = NULL
          WHERE id = ANY($1::int[])
          `,
          [deletedIds]
        );

        totalUpdated += deletedIds.length;
      }

      offset += BATCH_SIZE;

      if (rows.length < BATCH_SIZE) {
        break;
      }
    }

    console.info(
      JSON.stringify({
        job: jobName,
        durationMs: Date.now() - startTime,
        recordsProcessed: totalProcessed,
        filesDeleted,
        databaseRowsUpdated: totalUpdated,
      }),
      'Cron job completed'
    );

    return {
      recordsProcessed: totalProcessed,
      filesDeleted,
      databaseRowsUpdated: totalUpdated,
    };
  } catch (err) {
    console.error(
      JSON.stringify({
        job: jobName,
        err: err.message,
        stack: err.stack,
      }),
      'Cron job failed'
    );
    throw err;
  }
}

function setupCronJobs() {
  try {
    // Schedule cleanup job - runs every hour at minute 0
    cron.schedule('0 * * * *', async () => {
      if (cleanupRunning) {
        console.warn(
          JSON.stringify({
            job: 'proof-image-cleanup',
            message: 'Cleanup already running. Skipping...',
          })
        );
        return;
      }

      cleanupRunning = true;
      try {
        await runCleanupJob();
      } finally {
        cleanupRunning = false;
      }
    });

    cron.schedule('5 * * * *', async () => {
      if (reminderRunning) {
        console.warn(
          JSON.stringify({
            job: 'deadline-reminder',
            message: 'Reminder job already running. Skipping...',
          })
        );
        return;
      }

      reminderRunning = true;
      try {
        await runDeadlineReminderJob();
      } finally {
        reminderRunning = false;
      }
    });
  } catch (err) {
    console.error(
      JSON.stringify({
        job: 'cron-initialization',
        err: err.message,
        stack: err.stack,
      }),
      'Failed to initialize cron jobs'
    );
  }
}

module.exports = {
  setupCronJobs,
  runDeadlineReminderJob,
  runCleanupJob,
};
