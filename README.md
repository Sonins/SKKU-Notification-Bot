# SKKU 공지사항 봇 dag
Airflow dag를 이용한 성균관대 소프트웨어대학 공지사항 알람 봇입니다.  
## TODO
- [ ] Discord 봇 만들기
    - [ ] Discord Task 새로 만들기
    - [ ] Discord 메시지 포맷 빌드 함수 생성
- [ ] 공지사항 알람 주기 줄이기 (현재 1일)
- [ ] Postgresql 연동
- [ ] Slack 채널 이름 configuration에서 설정할 수 있게 조정
    - [ ] SlackWebHook 이용 operator 새로 만들기  
    혹은
    - [ ] PythonOpreator 이용 airflow.models.connection 모듈 이용해 추출
- [ ] 주석 추가