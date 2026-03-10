CREATE TABLE IF NOT EXISTS "__EFMigrationsHistory" (
    "MigrationId" character varying(150) NOT NULL,
    "ProductVersion" character varying(32) NOT NULL,
    CONSTRAINT "PK___EFMigrationsHistory" PRIMARY KEY ("MigrationId")
);

START TRANSACTION;


                ALTER TABLE "Tenant" ADD COLUMN IF NOT EXISTS "isActive" boolean NOT NULL DEFAULT TRUE;
                ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "isActive" boolean NOT NULL DEFAULT TRUE;
                ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "currentSessionId" text NULL;
                
                CREATE TABLE IF NOT EXISTS "FavoriteQuestion" (
                    "id" text NOT NULL,
                    "questionText" text NOT NULL,
                    "createdAt" timestamp with time zone NOT NULL,
                    "userId" text NULL,
                    CONSTRAINT "PK_FavoriteQuestion" PRIMARY KEY ("id"),
                    CONSTRAINT "FK_FavoriteQuestion_User_userId" FOREIGN KEY ("userId") REFERENCES "User" ("id") ON DELETE CASCADE
                );
                
                CREATE INDEX IF NOT EXISTS "IX_FavoriteQuestion_userId" ON "FavoriteQuestion" ("userId");
            

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260305200810_AddUserTenantActivityAndFavorites', '8.0.0');

COMMIT;

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

