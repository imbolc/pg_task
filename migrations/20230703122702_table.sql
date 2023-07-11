CREATE TABLE pg_task (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    step JSONB NOT NULL,
    wakeup_at timestamptz NOT NULL DEFAULT now(),
    tried INT NOT NULL DEFAULT 0,
    is_running BOOLEAN NOT NULL DEFAULT false,
    error TEXT,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX pg_task_wakeup_at_idx ON pg_task (wakeup_at);

COMMENT ON TABLE pg_task IS 'pg_task tasks';
COMMENT ON COLUMN pg_task.step IS 'State of the current step';
COMMENT ON COLUMN pg_task.wakeup_at IS 'Scheduled time for the task to execute the current step';
COMMENT ON COLUMN pg_task.tried IS 'Number of times the current step has resulted in an error';
COMMENT ON COLUMN pg_task.is_running IS 'Indicates if the current step is running right now';
COMMENT ON COLUMN pg_task.error IS 'Indicates if the current step has resulted in an error and all retry attempts have been exhausted. Set this field to null for the step to resume.';
COMMENT ON COLUMN pg_task.created_at IS 'Time the task was created';
COMMENT ON COLUMN pg_task.updated_at IS 'Time the task was updated';
