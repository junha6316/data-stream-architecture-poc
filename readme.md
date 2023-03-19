### 명령어 정리

## cassandra

cqlsh <host> <port> : 카산드라 shell 접근 없으면 brew install cassandra

- 키스페이스 확인
  USE your_keyspace;
  DESCRIBE TABLE your_table;

- 키스페이스 생성
  CREATE KEYSPACE stuff
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
  AND durable_writes = true;

- 테이블 생성
  CREATE TABLE stuff.weather (
  uuid UUID PRIMARY KEY,
  temp DOUBLE,
  humd DOUBLE,
  pres DOUBLE,
  device text;
  );

- column 추가
  ALTER TABLE weather ADD device text;

### 에러

List directory /var/lib/apt/lists/partial is missing. - Acquire (2: No such file or directory)

## 생각 정리

- stream으로 하면 안되고 a데이터를 저장하고 analysis 메세지 받으면 해야되나..?

### 유저 시나리오

1. 첫 연동

   1. 기존의 데이터를 다 가져와야함
   2. 데이터 수집(구매): 구매 관련된 데이터를 싹다 가져와서 redis에 저장
   3. 데이터 수집(유저): 유저 데이터 수집
   4. 데이터 분석: 유저 데이터를 가져오면서 해당 유저가 default 타임 윈도우안에 물건을 구매했는지 확인
      - first-touch, last-touch
      - 주문이나, 상품 단위로 묶여 있어야하나?(groupby)
      - 메세지와 상품 사이의 매핑이 되어 있어야함
      - 분석결과를 먼저 redis에 저장
   5. 데이터 수집(crm 메세지): 메세지 API 호출해서 데이터 수집(유저 데이터와 동시에 수집)
   6. 매핑된 값(메세지-프로덕트)를 기준으로 해당 메세지가 default 타임라인에 대해 얼마나 구매를 일으켰는지 반환
      - 메세지

   - 기획에 따라 달라지겠지만 먼저 상품이랑 메세지를 가져온 다음에 매핑 과정이 필요함
     - 메세지 분석해서 하는건 좀 어려울 수도
       - 가능은 하겠지만 다른 추적 툴 붙어있으면 좀 어려움
     - 매핑을 하려면 먼저 프로덕트랑 메세지 종류를 다 긁어와야함
     - 매핑

2. 동기화
   - 특정주기로 돌면서 데이터 수집을 진행하면서 검색결과를 업데이트 해야함
3. 분석 방법 변경
   ex) 타임 윈도우 변경

- 그냥 mongodb로 구현하는게 낫나..?

## ToDo

1. 한 10만건 정도로 가져오는데 얼마나 걸리는지 봐야됨
   10만건 / (2000건/1초) = 50초
   100만건 / (2000건/1초) = 500초
2. 스파크 운영 방법(?)
