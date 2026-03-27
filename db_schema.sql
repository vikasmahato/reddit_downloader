CREATE TABLE `posts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `reddit_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` text COLLATE utf8mb4_unicode_ci,
  `author` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `subreddit` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `permalink` varchar(512) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_utc` float DEFAULT NULL,
  `score` int DEFAULT NULL,
  `post_username` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `comments` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`id`),
  UNIQUE KEY `reddit_id` (`reddit_id`),
  UNIQUE KEY `permalink` (`permalink`),
  KEY `idx_subreddit` (`subreddit`),
  KEY `idx_author` (`author`)
) ENGINE=InnoDB AUTO_INCREMENT=90656 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `post_images` (
  `id` int NOT NULL AUTO_INCREMENT,
  `post_id` int DEFAULT NULL,
  `image_id` int DEFAULT NULL,
  `url` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_post_image` (`post_id`,`image_id`),
  KEY `image_id` (`image_id`),
  CONSTRAINT `post_images_ibfk_1` FOREIGN KEY (`post_id`) REFERENCES `posts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `post_images_ibfk_2` FOREIGN KEY (`image_id`) REFERENCES `images` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=90834 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `images` (
  `id` int NOT NULL AUTO_INCREMENT,
  `file_hash` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `file_path` text COLLATE utf8mb4_unicode_ci,
  `filename` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `file_size` bigint DEFAULT NULL,
  `download_date` date DEFAULT NULL,
  `download_time` time DEFAULT NULL,
  `is_deleted` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_hash` (`file_hash`),
  KEY `idx_hash` (`file_hash`)
) ENGINE=InnoDB AUTO_INCREMENT=90745 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `scrape_lists` (
  `id` int NOT NULL AUTO_INCREMENT,
  `type` enum('subreddit','user') COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `status` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'enabled',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_scraped_at` timestamp NULL DEFAULT NULL,
  `zero_result_count` int DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_type_name` (`type`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ALTER TABLE posts ADD COLUMN IF NOT EXISTS selftext TEXT NULL;
-- ALTER TABLE scrape_lists ADD COLUMN IF NOT EXISTS media_types VARCHAR(50) NOT NULL DEFAULT 'image,video' AFTER zero_result_count;
-- UPDATE scrape_lists SET media_types = 'image,video' WHERE media_types IS NULL OR media_types = '';
-- ALTER TABLE scrape_lists ADD COLUMN IF NOT EXISTS description TEXT NULL AFTER media_types;

-- Migration from old schema (enabled BOOLEAN -> status VARCHAR):
-- ALTER TABLE scrape_lists ADD COLUMN status VARCHAR(10) NOT NULL DEFAULT 'enabled' AFTER name;
-- UPDATE scrape_lists SET status = CASE WHEN enabled = TRUE THEN 'enabled' ELSE 'disabled' END;
-- ALTER TABLE scrape_lists DROP COLUMN enabled;