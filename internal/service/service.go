package service

import (
	"context"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"time"
)

const (
	MsgStatusInit = iota + 1
	MsgStatusSuccess
)

type bizFunc func(tx *gorm.DB) (Msg, error)

type Service struct {
	db       *gorm.DB
	producer sarama.SyncProducer
}

func (s *Service) ExecTx(ctx context.Context, biz bizFunc) error {
	var msg Msg
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		msg, err = biz(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixMilli()
		msg.UTime = now
		msg.CTime = now
		return tx.Create(&msg).Error
	})
	if err != nil {
		return err
	}
	err = s.SendMsg(ctx, msg)
	if err != nil {
		// 业务成功了，记录日志就可以
	}
	return nil
}

func (s *Service) StartAsyncTask() {
	const limit = 10
	for {
		now := time.Now().UnixMilli() - (3 * time.Second).Milliseconds()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

		var msgs []Msg
		err := s.db.WithContext(ctx).Where("status = ? AND u_time < ?", MsgStatusInit, now).
			Limit(limit).Offset(0).Find(&msgs).Error

		cancel()
		if err != nil {
			// 不结束异步任务
			continue
		}

		for _, msg := range msgs {
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			err = s.SendMsg(ctx, msg)
			cancel()
			if err != nil {
				// 记录日志，下一条
				continue
			}
		}

	}
}

func (s *Service) SendMsg(ctx context.Context, msg Msg) error {
	_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Where("id = ?", msg.Id).Updates(map[string]any{
		"status": MsgStatusSuccess,
		"u_time": time.Now().UnixMilli(),
	}).Error
}

type Msg struct {
	Id      int64
	Topic   string
	Content string

	UTime int64
	CTime int64
}
