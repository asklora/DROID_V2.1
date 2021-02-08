import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from hyperopt import STATUS_OK
from keras.callbacks import EarlyStopping
from tensorflow.python.keras import callbacks
from tensorflow.python.keras.layers import Input, Dense, Concatenate, Flatten, Conv2D, LeakyReLU, Reshape, Dropout
from tensorflow.python.keras.models import Model

from data.data_output import write_to_sql, write_to_sql_model_data


def full_model(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1,
               final_prediction2, args):
    lookback = args.lookback
    trainX1 = trainX1.reshape(-1, trainX1.shape[1], 1)

    input_shape = (lookback, 5, 5)
    input_img = Input(shape=input_shape)

    # input_shape2 = (lookback, 1)
    input_img2 = Input(shape=(lookback, 1))

    x_1 = Conv2D(args.cnn_hidden_size, (1, 5), strides=(1, 5), padding='valid', name='conv1', input_shape=input_shape)(
        input_img)
    x_1 = LeakyReLU(alpha=0.1)(x_1)

    # ASSUME n= 4
    x_1 = Conv2D(args.cnn_hidden_size * 2, (5, 1), strides=(5, 1), padding='valid', name='conv2')(x_1)
    x_1 = LeakyReLU(alpha=0.1)(x_1)

    # x_1 = Reshape((int(lookback), int(number_of_kernels * 2)))(x_1)
    # x_1 = Flatten()(x_1)
    x_1 = Reshape((x_1.shape[1] * x_1.shape[2] * x_1.shape[3], 1))(x_1)

    x_2 = Dense(args.num_nodes, input_shape=(lookback, 1))(input_img2)
    for i in range(args.num_layers - 1):
        x_2 = Dense(args.num_nodes)(x_2)
        x_2 = Dropout(args.dropout)(x_2)
    x_2 = Reshape((x_2.shape[1] * x_2.shape[2], 1))(x_2)

    # merge the final outputs of the GRU's
    x = Concatenate(axis=1)([x_1, x_2])

    x = Flatten()(x)
    target_dim = args.num_bins
    x = Dense(target_dim, activation='softmax')(x)
    adam = tf.keras.optimizers.Adam(lr=args.learning_rate)
    model = Model([input_img, input_img2], x)
    model.compile(adam, 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])

    model.summary()

    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

    # Checkpoint for saving the model.
    checkpoint = callbacks.ModelCheckpoint(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5",
                                           monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                           save_best_only=True,
                                           save_weights_only=True, period=1)
    callbacks_list = [checkpoint, es]

    history = model.fit(
        [trainX2, trainX1], trainY,
        validation_split=.2,
        batch_size=args.batch_size,
        epochs=args.epoch,
        callbacks=callbacks_list)
    # ******************************************************************************
    # ************************* Output to AWS **************************************

    # Predicting uo to 5 weeks for test dataset to report to AWS separately.
    predict_zeros = np.zeros((testX1.shape[1], 1))
    scores = np.zeros(5)
    for i in range(5):
        tempX1 = testX1[i, :, :]
        tempX2 = testX2[i, :, :, :, :]
        tempX1 = tempX1.reshape(-1, tempX1.shape[1], 1)
        tempX2 = tempX2.reshape(-1, tempX2.shape[1], 5, 5)
        tempY = testY[i, :, :]
        scores[i] = model.evaluate([tempX2, tempX1], tempY)[1]

    args.best_train_acc = max(history.history['sparse_categorical_accuracy'])
    args.best_valid_acc = max(history.history['val_sparse_categorical_accuracy'])
    args.test_acc_1 = scores[0]
    args.test_acc_2 = scores[1]
    args.test_acc_3 = scores[2]
    args.test_acc_4 = scores[3]
    args.test_acc_5 = scores[4]

    args.lowest_train_loss = min(history.history['loss'])
    args.lowest_valid_loss = min(history.history['val_loss'])
    args.best_valid_epoch = (np.asarray(history.history['val_sparse_categorical_accuracy'])).argmax()
    args.best_train_epoch = (np.asarray(history.history['sparse_categorical_accuracy'])).argmin()

    if args.save_plots:
        # Accuracy plots
        plt.figure()
        plt.plot(history.history['sparse_categorical_accuracy'])
        plt.plot(history.history['val_sparse_categorical_accuracy'])
        plt.title('model accuracy')
        plt.ylabel('accuracy')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        plt.savefig(args.plot_path + "plot_acc_" + str(int(args.timestamp)) + ".png")
        plt.close()

        # Loss plots
        plt.figure()
        plt.plot(history.history['loss'])
        plt.plot(history.history['val_loss'])
        plt.title('model loss')
        plt.ylabel('loss')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        plt.savefig(args.plot_path + "plot_loss_" + str(int(args.timestamp)) + ".png")
        plt.close()

    model.load_weights(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5")

    if args.test_output_flag:
        # Write the test data for each stock to AWS
        write_to_sql(testX1, testX2, testY, model, args.test_table_name, args)

    if args.production_output_flag:
        # Write the production data for each stock to AWS
        write_to_sql(final_prediction1, final_prediction2, None, model, args.production_table_name, args)

    # Write the model data to AWS
    write_to_sql_model_data(args)

    return {'loss': 1 - args.best_valid_acc, 'status': STATUS_OK}
