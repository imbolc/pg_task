CREATE FUNCTION pg_task_notify_on_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('pg_task_changed', '');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pg_task_changed
AFTER INSERT OR UPDATE
ON pg_task
FOR EACH ROW
EXECUTE PROCEDURE pg_task_notify_on_change();
