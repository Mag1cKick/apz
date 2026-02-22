# Rapport

# Setup

docker compose up --build

<img width="290" height="158" alt="image" src="https://github.com/user-attachments/assets/d793ec25-9363-4ca9-bdcd-f85bd56aaf27" />

# Interaction

1) adding a message
```
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/message" `
  -ContentType "application/json" `
  -Body (@{ msg = "hello" } | ConvertTo-Json)
```
<img width="797" height="161" alt="image" src="https://github.com/user-attachments/assets/e6901adb-81b5-4a10-a193-bfae8bcbd37c" />


2) Check logs
```
curl.exe "http://localhost:8001/logs"
```
<img width="929" height="32" alt="image_2026-02-22_12-33-08" src="https://github.com/user-attachments/assets/8c693dde-d546-43b9-a36d-66e8746c44cb" />


3) Check messages
```
curl.exe "http://localhost:8000/messages"
```
<img width="957" height="50" alt="image" src="https://github.com/user-attachments/assets/4f7324b4-733d-4aaf-bee2-c4445cd35ad4" />


4) Retry testing
```
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8000/message?fail_first_n=2" `
  -ContentType "application/json" `
  -Body (@{ msg = "retry demo" } | ConvertTo-Json)
```

<img width="797" height="161" alt="image" src="https://github.com/user-attachments/assets/82da41f1-e213-4c22-b0f5-a4bf42005f7b" />


5) Dedup testing
```
$id = "11111111-1111-1111-1111-111111111111"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8001/log" `
  -ContentType "application/json" `
  -Body (@{ id = $id; msg = "same id" } | ConvertTo-Json)

$id = "11111111-1111-1111-1111-111111111111"

Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8001/log" `
  -ContentType "application/json" `
  -Body (@{ id = $id; msg = "same id" } | ConvertTo-Json)
```
<img width="1017" height="377" alt="image" src="https://github.com/user-attachments/assets/fad3ecb4-4b44-4d95-9fe0-b3dd1b667d30" />
<img width="638" height="67" alt="image" src="https://github.com/user-attachments/assets/4b4ffcb1-dfb9-45c3-93c5-7f9efea9c4a6" />
