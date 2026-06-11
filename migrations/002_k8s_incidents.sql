ALTER TABLE incident_history
ADD COLUMN source_domain VARCHAR(32) DEFAULT 'ETL' NOT NULL;
