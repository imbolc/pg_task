ALTER TABLE pg_task
ADD COLUMN locked_by UUID,
ADD COLUMN lock_expires_at timestamptz;

DROP TRIGGER pg_task_changed ON pg_task;

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

CREATE OR REPLACE FUNCTION pg_task_notify_on_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('pg_task_changed', '');
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_task_changed_insert
AFTER INSERT
ON pg_task
FOR EACH ROW
EXECUTE PROCEDURE pg_task_notify_on_change();

CREATE TRIGGER pg_task_changed_delete
AFTER DELETE
ON pg_task
FOR EACH ROW
EXECUTE PROCEDURE pg_task_notify_on_change();

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
