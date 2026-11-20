CREATE TABLE [billing].[rate_plans] (
    [plan_code]          VARCHAR (20)    NOT NULL,
    [plan_name]          VARCHAR (80)    NOT NULL,
    [plan_type]          VARCHAR (10)    NOT NULL,
    [monthly_fee_mad]    DECIMAL (10, 2) NOT NULL,
    [included_minutes]   INT             NULL,
    [included_sms]       INT             NULL,
    [included_data_mb]   INT             NULL,
    [voice_rate_per_min] DECIMAL (8, 4)  NULL,
    [sms_rate]           DECIMAL (8, 4)  NULL,
    [data_rate_per_mb]   DECIMAL (8, 4)  NULL,
    [valid_from]         DATE            NOT NULL,
    [valid_to]           DATE            NULL,
    [is_active]          CHAR (1)        NOT NULL,
    [created_at]         DATETIME2 (0)   NOT NULL,
    CONSTRAINT [PK_rate_plans] PRIMARY KEY NONCLUSTERED ([plan_code] ASC)
);


GO

