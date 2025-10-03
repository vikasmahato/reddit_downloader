# Reddit Image Downloader

A Python script that downloads images from Reddit, including content that requires login authentication. Supports downloading from subreddits, direct URLs, and saved posts.

## Features

- 🔐 **Authentication Support**: Access private subreddits and saved posts
- 📱 **Multiple Sources**: Download from subreddits, direct URLs, or saved posts
- 🖼️ **Image Format Support**: JPG, PNG, GIF, WebP, WebM, BMP
- 📁 **Organized Downloads**: Images organized by subreddit
- 🔗 **URL Resolution**: Automatically resolves Imgur and Reddit URLs
- ⚡ **Batch Downloads**: Download multiple images efficiently

## Installation

1. Clone or download this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Setup

### 1. Create Reddit App Credentials

1. Go to [Reddit App Preferences](https://www.reddit.com/prefs/apps)
2. Click "Create App" or "Create Another App"
3. Fill in:
   - **Name**: `reddit_image_downloader` (or any name)
   - **App type**: Choose "script"
   - **Description**: `A script to download images from Reddit`
   - **Redirect URI**: `http://localhost:8080` (⚠️ Reddit requires this field but the script doesn't actually use it)
4. Click "Create app"
5. Note your **client ID** (the string under the app name) and **client secret** (the "secret" field)

### 2. Configure Credentials

1. Run the setup command:
```bash
python reddit_image_downloader.py --setup
```

2. Edit the created `config.ini` file with your credentials:

**For basic usage (public subreddits only):**
```ini
[reddit]
client_id = your_actual_client_id
client_secret = your_actual_client_secret
# username and password are optional for public content only
user_agent = reddit_image_downloader

[general]
download_folder = downloads
max_images_per_subreddit = 25
```

**For advanced features (saved posts, private subreddits):**
```ini
[reddit]
client_id = your_actual_client_id
client_secret = your_actual_client_secret
username = your_reddit_username
password = your_reddit_password
user_agent = reddit_image_downloader by u/your_username

[general]
download_folder = downloads
max_images_per_subreddit = 25
```

## Usage

### Authentication Modes

**🔓 Client Credentials Only (Minimal Setup):**
- Only need `client_id` and `client_secret`
- Can download from **public subreddits**
- Good for most users who just want images

**🔒 Full Authentication (Complete Setup):**
- Need `client_id`, `client_secret`, `username`, and `password`
- Can download from **private subreddits** and **saved posts**
- Required for personal/user-specific content

### Basic Downloads
```bash
# Download from a single subreddit
python reddit_image_downloader.py --subreddit wallpapers --limit 50

# Download from a specific user
python reddit_image_downloader.py --user naturephotographer --limit 25

# Download direct URLs
python reddit_image_downloader.py --urls "https://i.redd.it/example.jpg"
```

### Batch Scraping from Config

Configure your scraping lists in `config.ini`:

```ini
[scrape_list]
wallpapers
r/EarthPorn  
r/nature
r/photography

[user_scrape_list]
naturephotographer
u/GallowBoob
world_nature
```

Then run batch scraping:
```bash
# Scrape all configured subreddits and users
python reddit_image_downloader.py --scrape-all

# Scrape only subreddits from config
python reddit_image_downloader.py --scrape-subreddits

# Scrape only users from config
python reddit_image_downloader.py --scrape-users
```

### Download from Direct URLs (No Authentication Required)
```bash
python reddit_image_downloader.py --urls "https://i.redd.it/example.jpg" "https://imgur.com/example.png"
```

### Download Saved Posts (Requires Authentication)
```bash
python reddit_image_downloader.py --saved --limit 100
```

### Download from Private Subreddit
```bash
python reddit_image_downloader.py --subreddit private_subreddit_name --limit 25
```

## Command Line Options

- `--urls`: List of direct image URLs to download
- `--subreddit`: Subreddit name to download images from
- `--limit`: Maximum number of images to download (default: 25)
- `--saved`: Download from saved posts (requires authentication)
- `--config`: Path to config file (default: config.ini)
- `--setup`: Create default configuration file

## Examples

### Download Popular Wallpapers
```bash
python reddit_image_downloader.py --subreddit wallpapers --limit 100
```

### Download Photography Images
```bash
python reddit_image_downloader.py --subreddit earthporn --limit 50
```

### Download Memes
```bash
python reddit_image_downloader.py --subreddit memes --limit 30
```

### Download from Your Saved Posts
```bash
python reddit_image_downloader.py --saved --limit 50
```

## Output Structure

Images are downloaded to the `downloads` folder (configurable), organized by source:

```
downloads/
├── wallpapers/                           # Subreddit: r/wallpapers
│   ├── photographer123/                  # Username folder
│   │   ├── sunset_beach_20231215.jpg
│   │   └── mountain_view_deleted.png     # Deleted image marked
│   └── artist456/
│       └── modern_art.jpg
├── EarthPorn/                            # Subreddit: r/EarthPorn  
│   └── naturephotographer/
│       ├── desert_mountains.jpg
│       └── forest_lake.jpg
├── users/                                # User profile downloads
│   ├── naturephotographer/
│   │   └── portfolio_image_1.jpg
│   └── artist123/
│       └── digital_art.jpg
├── saved_posts/                          # Saved posts
│   └── image_1_20231215.jpg
└── metadata.db                           # SQLite metadata database
```

## Supported Image Sources

- **Reddit Images**: `i.redd.it`, `preview.redd.it`
- **Imgur**: `imgur.com`, `i.imgur.com`
- **Direct URLs**: Any direct image URL
- **Most image formats**: JPG, JPEG, PNG, GIF, BMP, WebP, WebM

## Troubleshooting

### Authentication Issues

**Google Authentication Users:**
- Reddit API doesn't support Google OAuth for scripts
- Options: 
  1. **Use client credentials only** (no username/password needed) for public content
  2. **Create an app password** in your Google account for Reddit access
  3. **Use Reddit username/email** + Google's app-specific password

**General Issues:**
- Verify your Reddit credentials in `config.ini`
- Ensure your Reddit account has access to the target subreddit
- Check if two-factor authentication is enabled (may require app password)
- **Note**: Redirect URI (`http://localhost:8080`) is only needed when creating the Reddit app - the script doesn't use it

### Download Failures
- Check your internet connection
- Some images may be hosted on slow or unreliable servers
- Verify the subreddit exists and has image posts

### Rate Limiting
- Reddit has rate limits on API requests
- The script includes built-in delays to respect these limits
- Consider reducing the `--limit` for very large downloads

## Security Notes

- Your Reddit credentials are stored in plaintext in `config.ini`
- Keep this file secure and never commit it to version control
- Consider using environment variables for production deployments

## License

This script is provided as-is for educational and personal use. Please respect Reddit's terms of service and the subreddit-specific rules when downloading content.
