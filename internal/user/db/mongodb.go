package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/ivanmalyi/WebService/internal/apperror"
	"github.com/ivanmalyi/WebService/internal/user"
	"github.com/ivanmalyi/WebService/pkg/logging"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type db struct {
	collection *mongo.Collection
	logger     *logging.Logger
}

func NewStorage(database *mongo.Database, collection string, logger *logging.Logger) user.Storage {
	return &db{
		collection: database.Collection(collection),
		logger:     logger,
	}
}

func (d *db) Create(ctx context.Context, user user.User) (string, error) {
	d.logger.Debug("Create user")
	result, err := d.collection.InsertOne(ctx, user)
	if err != nil {
		return "", fmt.Errorf("failed to create user error: %v", err)
	}
	d.logger.Debug("convert insertId to objId")

	objId, ok := result.InsertedID.(primitive.ObjectID)
	if ok {
		return objId.Hex(), nil
	}
	d.logger.Trace(user)

	return "", fmt.Errorf("failed to convert object to hex: %s", objId)
}

func (d *db) FindOne(ctx context.Context, id string) (u user.User, err error) {
	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return u, fmt.Errorf("failed to convert hex to object: %s", id)
	}
	filter := bson.M{"_id": objId}
	result := d.collection.FindOne(ctx, filter)

	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return u, apperror.ErrNotFound
		}
		return u, fmt.Errorf("filed to find one user by id: %s due to error: %v", id, result.Err())
	}

	if err = result.Decode(&u); err != nil {
		return u, fmt.Errorf("filed to decode user from db by id: %s due to error: %v", id, err)
	}
	return u, err
}

func (d *db) FindAll(ctx context.Context) (u []user.User, err error) {
	cursor, err := d.collection.Find(ctx, bson.M{})
	if cursor.Err() != nil {
		return u, fmt.Errorf("filed to find all users due to error: %v", cursor.Err())
	}

	if err = cursor.All(ctx, &u); err != nil {
		return u, fmt.Errorf("filed to read all users from db due to error: %v", err)
	}

	return u, err
}

func (d *db) Update(ctx context.Context, user user.User) error {
	objId, err := primitive.ObjectIDFromHex(user.ID)
	if err != nil {
		return fmt.Errorf("failed to convert hex to object: %s", user.ID)
	}

	filter := bson.M{"_id": objId}
	userBytes, err := bson.Marshal(user)
	if err != nil {
		return fmt.Errorf("filed to marshal user error: %v", err)
	}

	var updateUserObj bson.M
	err = bson.Unmarshal(userBytes, updateUserObj)
	if err != nil {
		return fmt.Errorf("filed to unmarshal user bytes: %v", err)
	}
	delete(updateUserObj, "_id")

	update := bson.M{
		"$set": updateUserObj,
	}
	result, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("filed to update user query. error: %v", err)
	}

	if result.MatchedCount == 0 {
		return apperror.ErrNotFound
	}
	d.logger.Trace("matched %d documents and modified %d", result.MatchedCount, result.ModifiedCount)

	return nil
}

func (d *db) Delete(ctx context.Context, id string) error {
	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("failed to convert hex to object: %s", id)
	}

	filter := bson.M{"_id": objId}
	result, err := d.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to execute query. error: %v", err)
	}
	if result.DeletedCount == 0 {
		return apperror.ErrNotFound
	}
	d.logger.Trace("deleted %d documents", result.DeletedCount)

	return nil
}
