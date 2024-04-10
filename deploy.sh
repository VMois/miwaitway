#!/bin/bash

if [ ! -f .env ]; then
    echo ".env is not found! Aborting!"
    exit 1
fi

git pull origin main
docker compose build
docker compose down && docker compose up -d
