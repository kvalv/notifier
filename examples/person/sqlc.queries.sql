-- name: CreatePerson :one
INSERT INTO person(name, age)
    VALUES (@name, @age)
RETURNING
    *;

