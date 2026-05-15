-- Add lease related columns
ALTER TABLE pg_task
ADD COLUMN locked_by UUID,
ADD COLUMN lock_expires_at timestamptz;

COMMENT ON COLUMN pg_task.locked_by IS 'Worker currently owning the running step lease';
COMMENT ON COLUMN pg_task.lock_expires_at IS 'Time when the running step lease expires and can be reclaimed';

ALTER TABLE pg_task
ADD CONSTRAINT pg_task_lease_state_check CHECK (
    (locked_by IS NULL AND lock_expires_at IS NULL)
    OR (locked_by IS NOT NULL AND lock_expires_at IS NOT NULL)
);

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

DROP INDEX pg_task_wakeup_at_idx;

-- Remove `is_running` column
UPDATE pg_task
SET locked_by = gen_random_uuid(),
    lock_expires_at = now()
WHERE is_running = true;

ALTER TABLE pg_task DROP COLUMN is_running;

-- Update trigger
--
-- The old trigger in migrations/20230714025134_trigger.sql:9 fired on every INSERT or UPDATE.
-- With leases, that became too noisy because heartbeat renewal updates only lock_expires_at.
-- If the old trigger stayed, every lease renewal would NOTIFY pg_task_changed,
-- waking waiting workers even though no new task became claimable.
DROP TRIGGER pg_task_changed ON pg_task;

CREATE TRIGGER pg_task_changed_insert
AFTER INSERT
ON pg_task
FOR EACH ROW
EXECUTE PROCEDURE pg_task_notify_on_change();

-- The new trigger only notifies on updates to fields that affect task state or claimability.
CREATE TRIGGER pg_task_changed_update
AFTER UPDATE
ON pg_task
FOR EACH ROW
WHEN (
    OLD.step IS DISTINCT FROM NEW.step
    OR OLD.wakeup_at IS DISTINCT FROM NEW.wakeup_at
    OR OLD.tried IS DISTINCT FROM NEW.tried
    OR OLD.error IS DISTINCT FROM NEW.error
    OR OLD.locked_by IS DISTINCT FROM NEW.locked_by
)
EXECUTE PROCEDURE pg_task_notify_on_change();
