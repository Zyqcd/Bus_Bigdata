# รัน scraping จากโปรเจกต์ที่เปิดใน VS Code

จาก Terminal ที่โฟลเดอร์ `D:\BigData\bangkok-traffic-data-pipeline`:

```powershell
.\Start-Project-Scraping.ps1
```

ไฟล์นี้เปิด Docker Compose ชุดเดิมที่เก็บข้อมูลอยู่ใน `C:\Users\acer\OneDrive\Documents\ChatGPT\Bigdata\project_runtime\airflow\compose.yaml` ไม่เปิดตัวเก็บซ้ำอีกชุด โค้ดตัวเก็บของเพื่อนในโปรเจกต์นี้ถูก mount ไปใช้โดย Docker ส่วน GPS และไฟล์ข้อมูลใหม่อยู่ใน `project_runtime\live_mobility` ของ workspace ดังกล่าว

- GPS จาก Namtang: สาย 1-21 (114) สองทิศทาง ทุก 1 นาที
- จราจร/อุบัติเหตุ: ทุก 15 นาที
- GTFS: ทุกวัน 06:00 เวลาไทย
- เวลาเดินทาง Longdo: เตรียมงานไว้ แต่จะข้ามหากไม่มี API key

ดูสถานะที่ http://localhost:8080 งานใช้ Airflow ทำเบื้องหลัง จึงไม่จำเป็นต้องเปิด Terminal ทิ้งไว้ แต่ต้องเปิดเครื่องและ Docker ตลอดช่วงที่ต้องการเก็บ

ข้อมูล GPS อยู่ PostgreSQL `namtang_db` schema `mobility_gps` และเก็บ raw/SQLite ใน `project_runtime\live_mobility\collected\gps` ยังไม่ได้เก็บ GPS รถทุกสายทั่วกรุงเทพฯ และไฟล์ Tableau แบบฝังข้อมูลไม่อัปเดตอัตโนมัติ
