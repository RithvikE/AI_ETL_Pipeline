"""Calls Ollama locally and lists models and downloads models as needed."""

import subprocess
import ollama
from openai import OpenAI
from config.settings import get_ollama_config, get_openai_config


def generate(prompt, provider="ollama"):
    """
    Send a prompt to the configured LLM provider and return the response.
    
    Args:
        prompt: The complete prompt string to send to the LLM
        provider: LLM provider name ("ollama" or "openai")
        
    Returns:
        str: Raw text response from the LLM
        
    Raises:
        Exception: If LLM generation fails with descriptive error message
    """
    provider = (provider or "ollama").strip().lower()

    try:
        if provider == "openai":
            config = get_openai_config()

            client_kwargs = {"api_key": config["api_key"]}
            if config.get("base_url"):
                client_kwargs["base_url"] = config["base_url"]

            client = OpenAI(**client_kwargs)
            response = client.responses.create(
                model=config["model"],
                input=prompt,
            )

            return response.output_text

        if provider != "ollama":
            raise ValueError(f"Unsupported LLM provider: {provider}")

        config = get_ollama_config()

        # Create Ollama client with configured host
        client = ollama.Client(host=config["host"])

        # Generate response from LLM
        response = client.generate(
            model=config["model"],
            prompt=prompt
        )

        return response["response"]
        
    except ollama.ResponseError as e:
        raise Exception(f"Ollama API error: {str(e)}")
    except Exception as e:
        raise Exception(f"Failed to generate LLM response ({provider}): {str(e)}")


def list_ollama_models():
    """Return installed Ollama model names."""
    try:
        result = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            check=True
        )

        models = set()
        lines = result.stdout.splitlines()
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            models.add(line.split()[0])

        return models, None
    except FileNotFoundError:
        return set(), "Ollama command not found. Please install Ollama and ensure it is available in PATH."
    except subprocess.CalledProcessError as e:
        details = (e.stderr or e.stdout or str(e)).strip()
        return set(), f"Failed to list Ollama models: {details}"


def download_ollama_model(model_name):
    """Download an Ollama model by name."""
    try:
        subprocess.run(
            ["ollama", "pull", model_name],
            capture_output=True,
            text=True,
            check=True
        )
        return True, f"Model '{model_name}' downloaded successfully."
    except FileNotFoundError:
        return False, "Ollama command not found. Please install Ollama and ensure it is available in PATH."
    except subprocess.CalledProcessError as e:
        details = (e.stderr or e.stdout or str(e)).strip()
        return False, f"Failed to download model '{model_name}': {details}"
    
