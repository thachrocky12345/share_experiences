create schema audit;


CREATE TABLE audit.audit_user (
    id bigserial primary key,
    username         VARCHAR,
    ip_address        text,
    payload        jsonb,
    created       timestamp not null default now()
);


CREATE OR REPLACE FUNCTION audit.add_partition_audit_user(p_day date)
        RETURNS void AS
        $BODY$
        DECLARE
            schemaName  	VARCHAR(30)  := 'audit';
            parentTableName 	VARCHAR(30)  := 'audit_user';
            childTableName    	VARCHAR(100) := parentTableName || '_' || TO_CHAR(p_day, 'yyyymmdd');
            pk           	VARCHAR(100) := 'id';
            tableExists 	INTEGER;

        BEGIN
            SELECT COUNT(*)
            INTO tableExists
            FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = schemaName
            AND c.relname = childTableName;

            IF tableExists = 0 THEN
                --standard
                EXECUTE 'CREATE table ' 				|| schemaName || '.' || childTableName || ' ( CHECK ( created >= DATE ''' || TO_CHAR(p_day, 'yyyy-mm-dd') || ''' AND created <  DATE ''' || TO_CHAR(p_day + 1, 'yyyy-mm-dd') || ''')) INHERITS (' || schemaName || '.'|| parentTableName || ')';
                EXECUTE 'CREATE INDEX ind_' 			|| schemaName || '_' || REPLACE(childTableName,'_','') || '_created_ind ON ' 				|| schemaName || '.' || childTableName || ' (created)';
                EXECUTE 'CREATE INDEX ind_' 			|| schemaName || '_' || REPLACE(childTableName,'_','') || '_username_ind ON ' 				|| schemaName || '.' || childTableName || ' (username)';


            END IF;
        END;
        $BODY$
          LANGUAGE plpgsql VOLATILE SECURITY DEFINER
          COST 100;


        GRANT EXECUTE ON FUNCTION audit.add_partition_audit_user(date) TO public;


CREATE OR REPLACE FUNCTION audit .insert_audit_user_partitionally()
        RETURNS trigger AS
        $BODY$
              DECLARE
                 schemaName  		VARCHAR(30) := 'audit';
                 parentTableName 	VARCHAR(30) := 'audit_user';
                 childTableName    	VARCHAR(100) := parentTableName || '_' || TO_CHAR(NEW.created::DATE, 'yyyymmdd');
              BEGIN

                 EXECUTE 'INSERT INTO ' || schemaName || '.' || childTableName || ' VALUES ($1.*)'  USING NEW  ;
                 RETURN NULL;

                 EXCEPTION
                WHEN UNIQUE_violation THEN
                    -- Do nothing for now as an error here rolls back all the attempted inserts AND we lose good data as well as bad
                    RETURN NULL;
                    WHEN undefined_table --undefined_table=42P01
                    THEN
                    -- try to create the partiiton if the DATE is not in the future

                    PERFORM audit.add_partition_audit_user(NEW.created::DATE);
                    -- attempt the insert again
                    EXECUTE 'INSERT INTO ' || schemaName || '.' || childTableName || ' VALUES ($1.*)' USING NEW;
                    RETURN NULL;
              END;
           $BODY$
          LANGUAGE plpgsql VOLATILE SECURITY DEFINER
          COST 100;


        GRANT EXECUTE ON FUNCTION audit.insert_audit_user_partitionally() TO public;

 CREATE TRIGGER trig_insert_user_audit_partition
      BEFORE INSERT
      ON audit.audit_user
      FOR EACH ROW
      EXECUTE PROCEDURE audit.insert_audit_user_partitionally()
       ;
