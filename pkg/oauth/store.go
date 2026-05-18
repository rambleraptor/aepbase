package oauth

import "database/sql"

func CreateOAuthIdentitiesTable(d *sql.DB) error {
	_, err := d.Exec(`CREATE TABLE IF NOT EXISTS _oauth_identities (
		provider TEXT NOT NULL,
		provider_user_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		email TEXT NOT NULL,
		create_time TEXT NOT NULL,
		PRIMARY KEY (provider, provider_user_id)
	)`)
	return err
}

func GetIdentity(d *sql.DB, provider, providerUserID string) (*Identity, error) {
	row := d.QueryRow(
		"SELECT provider, provider_user_id, user_id, email, create_time FROM _oauth_identities WHERE provider = ? AND provider_user_id = ?",
		provider, providerUserID,
	)
	var i Identity
	err := row.Scan(&i.Provider, &i.ProviderUserID, &i.UserID, &i.Email, &i.CreateTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &i, nil
}

func InsertIdentity(d *sql.DB, i *Identity) error {
	_, err := d.Exec(
		"INSERT INTO _oauth_identities (provider, provider_user_id, user_id, email, create_time) VALUES (?, ?, ?, ?, ?)",
		i.Provider, i.ProviderUserID, i.UserID, i.Email, i.CreateTime,
	)
	return err
}
