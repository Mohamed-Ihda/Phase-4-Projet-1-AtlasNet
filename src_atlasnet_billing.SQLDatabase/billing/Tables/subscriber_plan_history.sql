CREATE TABLE [billing].[subscriber_plan_history] (
    [history_id]     INT          NOT NULL,
    [subscriber_id]  INT          NOT NULL,
    [plan_code]      VARCHAR (20) NOT NULL,
    [effective_from] DATE         NOT NULL,
    [effective_to]   DATE         NULL,
    [change_reason]  VARCHAR (30) NULL,
    CONSTRAINT [PK_plan_history] PRIMARY KEY NONCLUSTERED ([history_id] ASC),
    CONSTRAINT [FK_hist_plan] FOREIGN KEY ([plan_code]) REFERENCES [billing].[rate_plans] ([plan_code]),
    CONSTRAINT [FK_hist_subscriber] FOREIGN KEY ([subscriber_id]) REFERENCES [billing].[subscribers] ([subscriber_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_hist_effective]
    ON [billing].[subscriber_plan_history]([effective_from] ASC, [effective_to] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_hist_subscriber]
    ON [billing].[subscriber_plan_history]([subscriber_id] ASC);


GO

