import tiktoken

tokenizer = tiktoken.get_encoding("cl100k_base")

def count_tokens(text):
        return len(tokenizer.encode(text))