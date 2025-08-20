from openai import OpenAI

model = "claude-3-5-haiku-20241022"
content = "Hello, what can you do?"
client = OpenAI(
    base_url="http://192.168.1.44:9080/v1",
    api_key="sk-i-love-you"
)

print(f"model: '{model}'")
print()
print(content)
print("--------------------------------")
try:
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": content}],
        stream=True
    )
    for chunk in response:
        content = chunk.choices[0].delta.content
        if content:
            print(content, end="", flush=True)
    print()

except Exception as e:
    print(f"An error occurred: {e}")
