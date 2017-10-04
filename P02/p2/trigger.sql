CREATE OR REPLACE FUNCTION update_numflights() RETURNS trigger AS $update_numflights$
		DECLARE
			old_status_count integer;
		BEGIN
		
			IF (TG_OP = 'INSERT') THEN
				SELECT numflights into old_status_count
				FROM numberofflightstaken
				WHERE customerid = NEW.customerid;
				IF EXISTS (SELECT customerid from numberofflightstaken
				    WHERE numberofflightstaken.customerid = NEW.customerid) THEN
					UPDATE numberofflightstaken
					SET numflights = numflights + 1
					WHERE numberofflightstaken.customerid = NEW.customerid;
				ELSE
					INSERT INTO numberofflightstaken
					(customerid,customername,numflights)
					values(NEW.customerid,
						(select name from customers
					WHERE customers.customerid = NEW.customerid),1);
				END IF;
		
			ELSEIF (TG_OP = 'DELETE' AND old_status_count = 1) THEN
				SELECT numflights into old_status_count
				FROM numberofflightstaken
				WHERE numberofflightstaken.customerid = OLD.customerid;

				DELETE FROM numberofflightstaken
				WHERE numberofflightstaken.customerid = OLD.customerid;
			ELSE 
				UPDATE numberofflightstaken
				SET numflights = numflights - 1
				WHERE numberofflightstaken.customerid = OLD.customerid;
			END IF;
		RETURN NULL;
		END;
$update_numflights$ LANGUAGE plpgsql;

CREATE TRIGGER update_numflights AFTER 
INSERT OR DELETE ON flewon 
FOR EACH ROW EXECUTE PROCEDURE update_numflights();
END;












