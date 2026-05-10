CREATE INDEX pg_task_locked_by_idx
ON pg_task (locked_by)
WHERE locked_by IS NOT NULL
  AND error IS NULL;

CREATE INDEX pg_task_next_available_at_idx
ON pg_task ((
    CASE
        WHEN locked_by IS NOT NULL THEN
            GREATEST(wakeup_at, lock_expires_at)
        ELSE
            wakeup_at
    END
))
WHERE error IS NULL;
