#!/bin/bash
curl -S -o POST -H "Authorization: token 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" \
-H "Accept: application/vnd.github.v3+json"\
    https://api.github.com/repos/asklora/DROID_V2.1/dispatches \
    --data '{"event_type": "build_image"}'
