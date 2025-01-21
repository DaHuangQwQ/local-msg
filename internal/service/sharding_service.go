package service

import (
	"context"
	"errors"
	"github.com/DaHuangQwQ/local-msg/internal/sharding"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"time"
)

type ShardingService struct {
	dbs      map[string]*gorm.DB
	producer sarama.SyncProducer

	sharding.Sharding
}

func (s *ShardingService) ExecTx(ctx context.Context, key string, biz bizFunc) error {
	dst := s.ShardingFunc(key)
	db, ok := s.dbs[dst.DB]
	if !ok {
		return errors.New("sharding service not exist")
	}

	var msg Msg
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err = biz(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixMilli()
		msg.UTime = now
		msg.CTime = now
		return tx.Table(dst.Table).Create(&msg).Error
	})
	if err != nil {
		return err
	}
	err = s.SendMsg(ctx, msg, db, dst.Table)
	if err != nil {
		// 业务成功了，记录日志就可以
	}
	return nil
}

func (s *ShardingService) StartAsyncTask() {
	const limit = 10

	dbs := s.EffectiveTablesFunc()
	for _, dst := range dbs {
		dst := dst
		go func() {
			db, ok := s.dbs[dst.DB]
			if !ok {
				return
			}
			table := dst.Table

			for {

				now := time.Now().UnixMilli() - (3 * time.Second).Milliseconds()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

				var msgs []Msg
				err := db.WithContext(ctx).Table(table).Where("status = ? AND u_time < ?", MsgStatusInit, now).
					Limit(limit).Offset(0).Find(&msgs).Error

				cancel()
				if err != nil {
					// 不结束异步任务
					continue
				}

				for _, msg := range msgs {
					ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
					err = s.SendMsg(ctx, msg, db, table)
					cancel()
					if err != nil {
						// 记录日志，下一条
						continue
					}
				}
			}

		}()

	}

}

func (s *ShardingService) SendMsg(ctx context.Context, msg Msg, db *gorm.DB, table string) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Table(table).Where("id = ?", msg.Id).Updates(map[string]any{
		"status": MsgStatusSuccess,
		"u_time": time.Now().UnixMilli(),
	}).Error
}
