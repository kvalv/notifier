migrate:
	PGPASSWORD=postgres psql -h localhost -U postgres -d notifier_mvp -f schema.sql

db:
	PGPASSWORD=postgres psql -h localhost -U postgres -d notifier_mvp

generate-sqlc:
	sqlc generate
