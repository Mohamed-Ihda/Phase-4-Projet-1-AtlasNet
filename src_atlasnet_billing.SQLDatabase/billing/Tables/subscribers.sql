CREATE TABLE [billing].[subscribers] (
    [subscriber_id]     INT             NOT NULL,
    [msisdn]            VARCHAR (15)    NOT NULL,
    [imsi]              VARCHAR (20)    NULL,
    [first_name]        VARCHAR (50)    NULL,
    [last_name]         VARCHAR (50)    NULL,
    [national_id]       VARCHAR (20)    NULL,
    [birth_date]        DATE            NULL,
    [gender]            CHAR (1)        NULL,
    [city]              VARCHAR (60)    NULL,
    [region]            VARCHAR (80)    NULL,
    [segment]           VARCHAR (20)    NULL,
    [customer_type]     VARCHAR (10)    NOT NULL,
    [current_plan_code] VARCHAR (20)    NOT NULL,
    [activation_date]   DATE            NOT NULL,
    [deactivation_date] DATE            NULL,
    [subscriber_status] VARCHAR (20)    NOT NULL,
    [credit_limit_mad]  DECIMAL (10, 2) NULL,
    [marketing_consent] CHAR (1)        NULL,
    [created_at]        DATETIME2 (0)   NULL,
    [updated_at]        DATETIME2 (0)   NULL,
    CONSTRAINT [PK_subscribers] PRIMARY KEY NONCLUSTERED ([subscriber_id] ASC),
    CONSTRAINT [CK_subscribers_status] CHECK ([subscriber_status]='DEACTIVATED' OR [subscriber_status]='SUSPENDED' OR [subscriber_status]='ACTIVE'),
    CONSTRAINT [CK_subscribers_type] CHECK ([customer_type]='POSTPAID' OR [customer_type]='PREPAID'),
    CONSTRAINT [FK_subscribers_plan] FOREIGN KEY ([current_plan_code]) REFERENCES [billing].[rate_plans] ([plan_code]),
    CONSTRAINT [UQ_subscribers_msisdn] UNIQUE NONCLUSTERED ([msisdn] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_subscribers_deact]
    ON [billing].[subscribers]([deactivation_date] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_subscribers_status]
    ON [billing].[subscribers]([subscriber_status] ASC)
    INCLUDE([customer_type], [segment], [city]);


GO

CREATE NONCLUSTERED INDEX [IX_subscribers_updated]
    ON [billing].[subscribers]([updated_at] ASC);


GO

