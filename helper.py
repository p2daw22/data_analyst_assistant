import ollama

def explain_profile(profile):
    prompt = "Explain this data profile in plain English. Keep it short and useful.\n\n"
    prompt = prompt + "Profile:\n" + str(profile)

    response = ollama.chat(
        model="llama3.1:8b",
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    answer = response["message"]["content"]
    return answer