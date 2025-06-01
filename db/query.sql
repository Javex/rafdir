-- name: GetProfile :one
SELECT * FROM profiles
WHERE profile = $1
LIMIT 1;

-- name: ListProfiles :many
SELECT * FROM profiles;

-- name: CreateProfile :one
INSERT INTO profiles (
  profile, lastCompletion
) VALUES (
  $1, $2
)
RETURNING *;

-- name: UpdateProfile :exec
UPDATE profiles
  set lastCompletion = $2
WHERE profile = $1
RETURNING *;

-- name: DeleteProfile :exec
DELETE FROM profiles
WHERE profile = $1;
