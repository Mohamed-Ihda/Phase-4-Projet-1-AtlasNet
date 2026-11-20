CREATE TABLE [billing].[invoices] (
    [invoice_id]        VARCHAR (20)    NOT NULL,
    [subscriber_id]     INT             NOT NULL,
    [period_start]      DATE            NOT NULL,
    [period_end]        DATE            NOT NULL,
    [rental_mad]        DECIMAL (10, 2) NOT NULL,
    [usage_charges_mad] DECIMAL (10, 2) NOT NULL,
    [vat_mad]           DECIMAL (10, 2) NOT NULL,
    [total_mad]         DECIMAL (10, 2) NOT NULL,
    [issue_date]        DATE            NOT NULL,
    [due_date]          DATE            NOT NULL,
    [payment_status]    VARCHAR (20)    NOT NULL,
    [paid_date]         DATE            NULL,
    CONSTRAINT [PK_invoices] PRIMARY KEY NONCLUSTERED ([invoice_id] ASC),
    CONSTRAINT [CK_invoices_period] CHECK ([period_end]>=[period_start]),
    CONSTRAINT [FK_invoices_subscriber] FOREIGN KEY ([subscriber_id]) REFERENCES [billing].[subscribers] ([subscriber_id])
);


GO

CREATE NONCLUSTERED INDEX [IX_invoices_period]
    ON [billing].[invoices]([period_start] ASC, [period_end] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_invoices_status]
    ON [billing].[invoices]([payment_status] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_invoices_subscriber]
    ON [billing].[invoices]([subscriber_id] ASC);


GO

