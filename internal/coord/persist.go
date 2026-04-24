package coord

import (
	"encoding/json"
	"fmt"

	"github.com/syzygyhack/ziggurat/internal/model"
	"go.etcd.io/bbolt"
)

var bucketTasks = []byte("tasks")

// Persist handles task state persistence via BoltDB transactions.
// Phase 0a uses direct BoltDB writes; Phase 0b will add a WAL.
type Persist struct {
	db *bbolt.DB
}

// NewPersist creates a persistence layer, ensuring the tasks bucket exists.
func NewPersist(db *bbolt.DB) (*Persist, error) {
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketTasks)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("init tasks bucket: %w", err)
	}
	return &Persist{db: db}, nil
}

// Save persists a task's current state.
func (p *Persist) Save(task *model.Task) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketTasks).Put([]byte(task.ID), data)
	})
}

// Load retrieves a task by ID.
func (p *Persist) Load(id string) (*model.Task, error) {
	var task model.Task
	err := p.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketTasks).Get([]byte(id))
		if data == nil {
			return fmt.Errorf("task not found: %s", id)
		}
		return json.Unmarshal(data, &task)
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// LoadAll returns all persisted tasks. Used for recovery on restart.
func (p *Persist) LoadAll() ([]*model.Task, error) {
	var tasks []*model.Task
	err := p.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTasks).ForEach(func(k, v []byte) error {
			var task model.Task
			if err := json.Unmarshal(v, &task); err != nil {
				return nil // skip corrupt entries
			}
			tasks = append(tasks, &task)
			return nil
		})
	})
	return tasks, err
}

// Delete removes a task from persistent storage.
func (p *Persist) Delete(id string) error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTasks).Delete([]byte(id))
	})
}
