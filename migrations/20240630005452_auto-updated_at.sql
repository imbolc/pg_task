CREATE OR REPLACE FUNCTION pg_task_before_update_refresh_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION pg_task_before_update_refresh_updated_at
IS 'Refreshes `updated_at` column of the task';


CREATE TRIGGER pg_task_before_update_refresh_updated_at_trigger
BEFORE UPDATE ON pg_task
FOR EACH ROW EXECUTE FUNCTION pg_task_before_update_refresh_updated_at();

COMMENT ON TRIGGER pg_task_before_update_refresh_updated_at_trigger
ON pg_task
IS 'Refreshes `updated_at` column of the task on change';

