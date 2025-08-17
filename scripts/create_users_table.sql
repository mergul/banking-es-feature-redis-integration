-- Users Table Creation Script
-- This script creates the users table

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Create indexes for users table
CREATE INDEX IF NOT EXISTS idx_users_username
    ON users (username)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_users_email
    ON users (email)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_users_is_active
    ON users (is_active)
    WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS idx_users_created_at
    ON users (created_at)
    WITH (fillfactor = 90);

-- Add unique constraints
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_user_username'
    ) THEN
        ALTER TABLE users ADD CONSTRAINT unique_user_username
            UNIQUE (username);
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_user_email'
    ) THEN
        ALTER TABLE users ADD CONSTRAINT unique_user_email
            UNIQUE (email);
    END IF;
END$$;

-- Create trigger for updated_at
DROP TRIGGER IF EXISTS trigger_update_users_updated_at ON users;
CREATE TRIGGER trigger_update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Print success message
DO $$
BEGIN
    RAISE NOTICE 'Users table created successfully!';
    RAISE NOTICE 'Indexes: username, email, is_active, created_at';
    RAISE NOTICE 'Constraints: unique_user_username, unique_user_email';
    RAISE NOTICE 'Triggers: trigger_update_users_updated_at';
END $$; 