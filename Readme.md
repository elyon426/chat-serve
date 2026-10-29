//BELOW IS THE SCHEMA FOR OUR DATABASE
```
-- 1. Users Table (Core profiles)
CREATE TABLE users (
    user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    phone_number VARCHAR(20) UNIQUE NOT NULL,
    username VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Groups Table (Core chat structures)
CREATE TABLE groups (
    group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Group Members Table (Simple room membership mappings)
CREATE TABLE group_members (
    group_id UUID REFERENCES groups(group_id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(user_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL DEFAULT 'member', -- 'admin', 'member'
    PRIMARY KEY (group_id, user_id)
);

-- 4. Messages History Table (Lean single-source log with media support)
CREATE TABLE messages (
    message_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    receiver_id UUID NOT NULL, -- Can point directly to a user_id or a group_id
    content TEXT,              -- Kept nullable in case it's a media-only item (e.g., sending just a photo)
    media_url TEXT,            -- Preserved for image, video, or voice note attachments
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexing for high-speed chronological message retrieval
CREATE INDEX idx_messages_receiver_time ON messages(receiver_id, timestamp DESC);
```
//we must handle 2 devices
-> allow user to register another devices in settings and they should have the same data in the new device 