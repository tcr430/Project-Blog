# Article Image Provider Note

## What changed
- Article image generation is no longer hard-wired to OpenAI.
- The provider path now supports a primary provider plus an automatic fallback provider.
- Production defaults are:
  - primary provider: `flux`
  - FLUX model: `flux-2-max`
  - fallback provider: `openai`
  - OpenAI fallback model: `gpt-image-1`

## How provider selection works
- `pipeline/scripts/generate_images.py` resolves provider settings from CLI arguments first, then environment variables, then defaults.
- The weekly pipeline passes those settings through to image generation.
- Supported config values:
  - `IMAGE_PROVIDER`
  - `IMAGE_FALLBACK_PROVIDER`
  - `IMAGE_ALLOW_FALLBACK`
  - `FLUX_IMAGE_MODEL`
  - `OPENAI_IMAGE_MODEL`
  - `IMAGE_FALLBACK_MODEL`
  - `FLUX_TIMEOUT_SECONDS`
  - `FLUX_POLL_INTERVAL_SECONDS`
  - `BFL_API_KEY`
  - `OPENAI_API_KEY`

## How FLUX is called
- FLUX uses the Black Forest Labs asynchronous API.
- The generator:
  1. submits a request to `https://api.bfl.ai/v1/<model-endpoint>`
  2. reads the returned `polling_url`
  3. polls until the job is `Ready`, `Failed`, or times out
  4. downloads the image from `result.sample`

## How fallback works
- The generator tries the primary provider first.
- If the provider is unavailable, times out, returns invalid bytes, fails validation, or raises an API error, the system logs the reason and then tries the fallback provider if fallback is enabled.
- Fallback events are recorded in the image-generation report and per-image detail rows.

## Validation behavior
- Every generated image must pass basic validation before being accepted:
  - non-empty bytes
  - decodable image
  - acceptable aspect ratio for the requested size
- Hero images still use the existing OpenAI-based QA pass when `OPENAI_API_KEY` is available.
- Section images rely on the deterministic basic validation path for speed.

## How to test

### FLUX only
```powershell
python pipeline/scripts/generate_images.py pipeline/data/your-article-package.json --provider flux --fallback-provider none
```

### OpenAI only
```powershell
python pipeline/scripts/generate_images.py pipeline/data/your-article-package.json --provider openai --fallback-provider none
```

### FLUX with OpenAI fallback
```powershell
python pipeline/scripts/generate_images.py pipeline/data/your-article-package.json --provider flux --fallback-provider openai
```

### Force fallback during testing
- Temporarily unset `BFL_API_KEY`, or set `--provider flux --fallback-provider openai` without valid FLUX credentials.
- The logs should show the FLUX failure and the OpenAI fallback success.
