#!/bin/bash
#
# Reddit Downloader Service Script
# 
# This script runs the Reddit downloader in loop mode and periodically
# updates comments for posts in the database.
#
# Usage:
#   ./run_reddit_service.sh [options]
#
# Options:
#   --comment-interval MINUTES      Interval between comment updates (default: 60)
#   --comment-limit N              Number of posts to update comments for (default: 10000)
#   --config PATH                  Path to config.ini (default: config.ini)
#   --log-dir PATH                 Directory for log files (default: ./logs)
#   --stop                         Stop running service
#   --status                       Check service status
#

set -euo pipefail

# Default configuration
COMMENT_INTERVAL=60
COMMENT_LIMIT=10000
CONFIG_FILE="config.ini"
LOG_DIR="./logs"
PID_FILE="./.reddit_service.pid"
DOWNLOAD_LOG="$LOG_DIR/downloader.log"
COMMENT_LOG="$LOG_DIR/comments.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
STOP_SERVICE=false
CHECK_STATUS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --comment-interval)
            COMMENT_INTERVAL="$2"
            shift 2
            ;;
        --comment-limit)
            COMMENT_LIMIT="$2"
            shift 2
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --stop)
            STOP_SERVICE=true
            shift
            ;;
        --status)
            CHECK_STATUS=true
            shift
            ;;
        -h|--help)
            cat << EOF
Reddit Downloader Service Script

Usage: $0 [options]

Options:
  --comment-interval MINUTES      Interval between comment updates (default: 60)
  --comment-limit N              Number of posts to update comments for (default: 10000)
  --config PATH                  Path to config.ini (default: config.ini)
  --log-dir PATH                 Directory for log files (default: ./logs)
  --stop                         Stop running service
  --status                       Check service status
  -h, --help                     Show this help message

Note: The downloader runs with --loop flag, which automatically scrapes
      every 5 minutes. The download-interval option is not used.

Examples:
  $0                                    # Start with default settings
  $0 --comment-interval 30              # Update comments every 30 minutes
  $0 --comment-limit 5000               # Update last 5000 posts
  $0 --stop                             # Stop the service
  $0 --status                           # Check if service is running
EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Function to print colored messages
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Check if service is running
is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE" 2>/dev/null)
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0
        else
            # PID file exists but process is dead
            rm -f "$PID_FILE"
            return 1
        fi
    fi
    return 1
}

# Stop the service
stop_service() {
    if ! is_running; then
        log_warning "Service is not running"
        return 0
    fi
    
    local pid=$(cat "$PID_FILE")
    log_info "Stopping service (PID: $pid)..."
    
    # Kill the main process and its children
    pkill -P "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
    
    # Wait a bit for graceful shutdown
    sleep 2
    
    # Force kill if still running
    if ps -p "$pid" > /dev/null 2>&1; then
        log_warning "Force killing process..."
        kill -9 "$pid" 2>/dev/null || true
    fi
    
    rm -f "$PID_FILE"
    log_success "Service stopped"
}

# Check service status
check_status() {
    if is_running; then
        local pid=$(cat "$PID_FILE")
        log_success "Service is running (PID: $pid)"
        
        # Show process tree
        echo ""
        echo "Process tree:"
        pstree -p "$pid" 2>/dev/null || ps -f -p "$pid" 2>/dev/null || true
        
        # Show log file sizes
        echo ""
        echo "Log files:"
        [[ -f "$DOWNLOAD_LOG" ]] && echo "  Downloader: $DOWNLOAD_LOG ($(du -h "$DOWNLOAD_LOG" | cut -f1))"
        [[ -f "$COMMENT_LOG" ]] && echo "  Comments: $COMMENT_LOG ($(du -h "$COMMENT_LOG" | cut -f1))"
        
        return 0
    else
        log_warning "Service is not running"
        return 1
    fi
}

# Handle signals
cleanup() {
    log_info "Received shutdown signal, cleaning up..."
    stop_service
    exit 0
}

trap cleanup SIGINT SIGTERM

# Main service function
run_service() {
    log_info "Starting Reddit Downloader Service"
    log_info "Downloader: Running with --loop flag (scrapes every 5 minutes)"
    log_info "Comment update interval: $COMMENT_INTERVAL minutes"
    log_info "Comment limit: $COMMENT_LIMIT posts"
    log_info "Config file: $CONFIG_FILE"
    log_info "Log directory: $LOG_DIR"
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Check if config file exists
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Config file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Check if already running
    if is_running; then
        log_error "Service is already running (PID: $(cat "$PID_FILE"))"
        log_info "Use --stop to stop the service first"
        exit 1
    fi
    
    # Save PID
    echo $$ > "$PID_FILE"
    log_success "Service started (PID: $$)"
    
    # Convert intervals to seconds
    local comment_interval_sec=$((COMMENT_INTERVAL * 60))
    
    # Start downloader in loop mode (runs --scrape-all every 5 minutes)
    log_info "Starting downloader in loop mode..."
    reddit-downloader --scrape-all --loop --config "$CONFIG_FILE" >> "$DOWNLOAD_LOG" 2>&1 &
    local download_pid=$!
    log_success "Downloader started (PID: $download_pid)"
    
    # Track last comment update time
    local last_comment=$(date +%s)
    local current_time
    
    # Main loop for comment updates
    log_info "Starting comment update scheduler..."
    while true; do
        current_time=$(date +%s)
        
        # Check if downloader is still running
        if ! ps -p "$download_pid" > /dev/null 2>&1; then
            log_error "Downloader process died, restarting..."
            reddit-downloader --scrape-all --loop --config "$CONFIG_FILE" >> "$DOWNLOAD_LOG" 2>&1 &
            download_pid=$!
            log_success "Downloader restarted (PID: $download_pid)"
        fi
        
        # Check if it's time to update comments
        local time_since_comment=$((current_time - last_comment))
        if [[ $time_since_comment -ge $comment_interval_sec ]]; then
            log_info "Starting comment update cycle..."
            reddit-update-comments --limit "$COMMENT_LIMIT" --config "$CONFIG_FILE" >> "$COMMENT_LOG" 2>&1 &
            local comment_pid=$!
            last_comment=$(date +%s)
            
            # Wait for comment update to finish (but don't block forever)
            local wait_count=0
            while ps -p "$comment_pid" > /dev/null 2>&1 && [[ $wait_count -lt 3600 ]]; do
                sleep 1
                wait_count=$((wait_count + 1))
            done
            
            if ps -p "$comment_pid" > /dev/null 2>&1; then
                log_warning "Comment update is taking longer than expected, continuing..."
            else
                log_success "Comment update completed"
            fi
        fi
        
        # Sleep for a short interval before checking again
        sleep 30
    done
}

# Handle stop command
if [[ "$STOP_SERVICE" == true ]]; then
    stop_service
    exit 0
fi

# Handle status command
if [[ "$CHECK_STATUS" == true ]]; then
    check_status
    exit $?
fi

# Start the service
run_service

