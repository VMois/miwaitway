#!/bin/bash

git pull origin main
docker compose build
docker compose down && docker compose up -d
