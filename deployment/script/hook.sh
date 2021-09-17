#!/bin/bash
curl -H "Authorization: token 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" \
    --request POST \
    --data '{"event_type": "build_image"}' \
    https://api.github.com/repos/asklora/DROID_V2.1/dispatches
