# Google Authentication Guide for Reddit API

If you use Google authentication for Reddit, here are your options:

## 🔓 Option 1: Client Credentials Only (Recommended)

**Skip username/password entirely!** This works for most users.

### Setup:
1. Get your `client_id` and `client_secret` from [Reddit Apps](https://www.reddit.com/prefs/apps)
2. Configure only these in `config.ini`:

```ini
[reddit]
client_id = your_client_id_from_reddit_app
client_secret = your_client_secret_from_reddit_app
user_agent = reddit_image_downloader
```

### What you can do:
- ✅ Download from **public subreddits** (wallpapers, earthporn, memes, etc.)
- ✅ Download **direct image URLs**
- ✅ Access **all public Reddit content**

### What you CAN'T do:
- ❌ Download saved posts (`--saved` option)
- ❌ Access private subreddits
- ❌ User-specific features

## 🔒 Option 2: Create App Password (Advanced Users)

If you need saved posts or private subreddit access:

### Steps:
1. Go to your [Google Account Security](https://myaccount.google.com/security)
2. Enable 2-Step Verification if not already enabled
3. Go to "App passwords"
4. Generate an app password for "Reddit"
5. Use these credentials in `config.ini`:

```ini
[reddit]
client_id = your_client_id_from_reddit_app
client_secret = your_client_secret_from_reddit_app
username = your_reddit_username_or_email
password = the_16_character_app_password_from_google
user_agent = reddit_image_downloader by u/your_username
```

## 🎯 Recommendation

**For 90% of users:** Use Option 1 (client credentials only). This gives you access to download images from popular subreddits without any password complications.

Only use Option 2 if you specifically need to download your saved posts or access private subreddits.

## Quick Test

Try this command to test client credentials only:
```bash
python reddit_image_downloader.py --subreddit wallpapers "--limit 5"
```

If it works, you don't need username/password for basic image downloading!
