CREATE TABLE IF NOT EXISTS person(
    id serial PRIMARY KEY,
    name varchar(100) NOT NULL,
    age int NOT NULL
);

CREATE OR REPLACE FUNCTION notify_person_created()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM
        pg_notify('person', NEW.id::text);
        -- from [docs](https://www.postgresql.org/docs/current/plpgsql-trigger.html):
        -- > The return value of a row-level trigger fired AFTER [...] is always ignored; it might as well be null. 
    RETURN null;
END;
$$;
CREATE OR REPLACE TRIGGER person_notify_trigger
    AFTER INSERT ON person
    FOR EACH ROW
    EXECUTE PROCEDURE notify_person_created();

DELETE FROM person;
