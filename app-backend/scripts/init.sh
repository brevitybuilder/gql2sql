#!/bin/sh

# Start gotrue in background
gotrue &

# Start the app in foreground
app-backend
