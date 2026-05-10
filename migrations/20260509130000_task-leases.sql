ALTER TABLE pg_task
ADD COLUMN locked_by UUID,
ADD COLUMN lock_expires_at timestamptz;

UPDATE pg_task
SET locked_by = gen_random_uuid(),
    lock_expires_at = now()
WHERE is_running = true;

ALTER TABLE pg_task
ADD CONSTRAINT pg_task_lease_state_check CHECK (
    (locked_by IS NULL AND lock_expires_at IS NULL)
    OR (locked_by IS NOT NULL AND lock_expires_at IS NOT NULL)
);

ALTER TABLE pg_task DROP COLUMN is_running;

COMMENT ON COLUMN pg_task.locked_by IS 'Worker currently owning the running step lease';
COMMENT ON COLUMN pg_task.lock_expires_at IS 'Time when the running step lease expires and can be reclaimed';
