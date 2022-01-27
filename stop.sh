#!/bin/bash

ps -ef | grep easyid | grep -v grep | awk '{print $2}' | xargs kill -9