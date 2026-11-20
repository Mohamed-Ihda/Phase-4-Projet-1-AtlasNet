CREATE TABLE [billing].[network_cells] (
    [cell_id]           VARCHAR (20)   NOT NULL,
    [site_id]           VARCHAR (30)   NOT NULL,
    [site_name]         VARCHAR (100)  NULL,
    [city]              VARCHAR (60)   NULL,
    [region]            VARCHAR (80)   NULL,
    [latitude]          DECIMAL (9, 6) NULL,
    [longitude]         DECIMAL (9, 6) NULL,
    [technology]        VARCHAR (5)    NULL,
    [azimuth_deg]       INT            NULL,
    [commissioned_date] DATE           NULL,
    [cell_status]       VARCHAR (20)   NULL,
    CONSTRAINT [PK_network_cells] PRIMARY KEY NONCLUSTERED ([cell_id] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_cells_city]
    ON [billing].[network_cells]([city] ASC);


GO

CREATE NONCLUSTERED INDEX [IX_cells_tech]
    ON [billing].[network_cells]([technology] ASC);


GO

