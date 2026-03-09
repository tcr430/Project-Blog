# Project instructions

## Stack
- Windows development
- Python backend pipeline
- Jekyll blog in /blog
- GitHub Actions for automation

## Key folders
- /pipeline/scripts = Python automation scripts
- /pipeline/prompts = prompt templates
- /pipeline/data = JSON state files
- /blog/_posts = generated markdown posts
- /blog/assets/img = generated article images

## Rules
- Load secrets from the root .env file
- Never hardcode API keys
- Prefer small, focused files
- Keep scripts readable for a beginner
- Use pathlib for file paths
- Save generated blog posts in Jekyll-compatible markdown
- Do not change unrelated files