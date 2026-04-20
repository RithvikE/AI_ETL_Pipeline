"""Model-name normalization utilities."""


def normalize_model_name(model_name):
    """Normalize Ollama model name for reliable matching."""
    return (model_name or "").strip().lower()


def candidate_model_names(model_name):
    """Return acceptable aliases for a configured model name."""
    normalized = normalize_model_name(model_name)
    if not normalized:
        return set()

    candidates = {normalized}
    if ":" in normalized:
        candidates.add(normalized.split(":", 1)[0])
    else:
        candidates.add(f"{normalized}:latest")

    return candidates
