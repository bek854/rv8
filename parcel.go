package main

import (
	"database/sql"
	"time"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec(
		`INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?)`,
		p.Client, p.Status, p.Address, p.CreatedAt,
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	p.Number = int(id)
	_ = s.AddHistory(p)
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	row := s.db.QueryRow(
		`SELECT number, client, status, address, created_at FROM parcel WHERE number = ?`, number,
	)
	var p Parcel
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query(
		`SELECT number, client, status, address, created_at FROM parcel WHERE client = ?`, client,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Parcel
	for rows.Next() {
		var p Parcel
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec(
		`UPDATE parcel SET status = ? WHERE number = ?`, status, number,
	)
	if err != nil {
		return err
	}
	p, err := s.Get(number)
	if err == nil {
		_ = s.AddHistory(p)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	row := s.db.QueryRow(`SELECT status FROM parcel WHERE number = ?`, number)
	var status string
	if err := row.Scan(&status); err != nil {
		return err
	}
	if status != ParcelStatusRegistered {
		return nil
	}
	_, err := s.db.Exec(
		`UPDATE parcel SET address = ? WHERE number = ?`, address, number,
	)
	if err != nil {
		return err
	}
	p, err := s.Get(number)
	if err == nil {
		_ = s.AddHistory(p)
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	row := s.db.QueryRow(`SELECT status FROM parcel WHERE number = ?`, number)
	var status string
	if err := row.Scan(&status); err != nil {
		return err
	}
	if status != ParcelStatusRegistered {
		return nil
	}
	_, err := s.db.Exec(`DELETE FROM parcel WHERE number = ?`, number)
	return err
}

// --- Методы для истории ---

func (s ParcelStore) AddHistory(p Parcel) error {
	_, err := s.db.Exec(
		`INSERT INTO parcel_history (parcel_number, client, address, status, changed_at) VALUES (?, ?, ?, ?, ?)`,
		p.Number, p.Client, p.Address, p.Status, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s ParcelStore) GetHistoryByClient(client int) ([]ParcelHistory, error) {
	rows, err := s.db.Query(
		`SELECT id, parcel_number, client, address, status, changed_at FROM parcel_history WHERE client = ? ORDER BY id`, client,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []ParcelHistory
	for rows.Next() {
		var h ParcelHistory
		if err := rows.Scan(&h.ID, &h.ParcelNum, &h.Client, &h.Address, &h.Status, &h.ChangedAt); err != nil {
			return nil, err
		}
		res = append(res, h)
	}
	return res, nil
}
