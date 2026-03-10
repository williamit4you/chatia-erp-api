START TRANSACTION;

ALTER TABLE "User" ADD "hasDashboardAccess" boolean NOT NULL DEFAULT FALSE;

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260310111948_AddHasDashboardAccess', '8.0.0');

COMMIT;

START TRANSACTION;

ALTER TABLE "User" ADD "hasPayableAccess" boolean NOT NULL DEFAULT FALSE;

ALTER TABLE "User" ADD "hasReceivableAccess" boolean NOT NULL DEFAULT FALSE;

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260310122053_AddPayableReceivableAccess', '8.0.0');

COMMIT;

