DROP TRIGGER pg_task_changed ON pg_task;

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
