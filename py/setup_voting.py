import random
import time
import psycopg2
import simplejson as json
from datetime import datetime
from confluent_kafka import SerializingProducer, DeserializingConsumer, Consumer, KafkaException, KafkaError
from setup_candidates_voters import delivery_report

# Variables Setup
producer = SerializingProducer({'bootstrap.servers': 'localhost:29092'})
consumer = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cursor = conn.cursor()

    # Grab all the Candidates
    cursor.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) as t;
    """)
    candidates = [candidate[0] for candidate in cursor.fetchall()]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")

    result = []
    consumer.subscribe(["voters_topic"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }

                try:
                    print('User {} is voting for candidate: {}'.format(vote['voter_id'], vote['candidate_id']))
                    cursor.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES (%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.flush()
                except Exception as e:
                    print(e)

            time.sleep(0.5)

    except Exception as e:
        print(e)