import keras.backend
import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from hyperopt import STATUS_OK
from keras.callbacks import EarlyStopping
from tensorflow.python.keras import callbacks
from tensorflow.python.keras.layers import Input, Dense, Concatenate, \
    LeakyReLU, Reshape, Dropout, Conv3D, Flatten
from tensorflow.python.keras.models import Model

from dlpa.data.data_output import write_to_sql, write_to_sql_model_data
from dlpa.data.data_preprocess import test_data_reshape
from dlpa.model.ec2_fns import save_to_ec2, load_from_ec2
from global_vars import candle_type_candles

def full_model(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1,
               final_prediction2, indices_df, args, hypers):
    if args.test_num != 0:
        # Removing the nan rows in test datasets
        args.test_mask = np.squeeze(args.test_mask)
        testX1 = testX1[:, args.test_mask, :]
        if testX2 is not None:
            testX2 = testX2[:, args.test_mask, :, :, :, :]
        testY = testY[:, args.test_mask, :]

    if args.cnn_kernel_size != 0:
        model, history = model_returns_candles(trainX1, trainX2, trainY, validX1, validX2, validY, args, hypers)
    else:
        model, history = model_returns(trainX1, trainY, validX1, validY, args, hypers)

    # train_num = 0 means going to inference mode.
    if args.train_num != 0:
        if args.save_ec2:
            save_to_ec2(args)

    if args.train_num != 0:
        model.load_weights(args.model_path + args.model_filename)
    else:
        load_from_ec2(args)
        model.load_weights(args.model_path + args.model_filename)
    # ******************************************************************************
    # ************************* Output to AWS **************************************

    # Predicting uo to 5 weeks for test dataset to report to AWS separately.
    if testX1 is not None:
        scores = np.zeros(5)
        tempX1, tempX2 = test_data_reshape(testX1, testX2, args, hypers)
        if args.cnn_kernel_size != 0:
            for i in range(len(testX1)):
                scores[i] = model.evaluate([tempX2[i], tempX1[i]], testY[i])[1]
        else:
            for i in range(len(testX1)):
                scores[i] = model.evaluate(tempX1[i], testY[i])[1]

        args.test_acc_1 = scores[0]
        args.test_acc_2 = scores[1]
        args.test_acc_3 = scores[2]
        args.test_acc_4 = scores[3]
        args.test_acc_5 = scores[4]
    else:
        args.test_acc_1 = None
        args.test_acc_2 = None
        args.test_acc_3 = None
        args.test_acc_4 = None
        args.test_acc_5 = None

    if args.train_num != 0:
        args.best_train_acc = max(history.history['sparse_categorical_accuracy'])
        args.best_valid_acc = max(history.history['val_sparse_categorical_accuracy'])

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

    # if args.test_output_flag:
    #     # Write the test data for each stock to AWS
    #     write_to_sql(testX1, testX2, testY, model, args.test_table_name, args)

    # Write the production data for each stock to AWS
    if args.go_live:
        if args.cnn_kernel_size != 0:
            predicted_values = model.predict([final_prediction2, final_prediction1.astype(float)])
        else:
            predicted_values = model.predict(final_prediction1.astype(float))
    else:
        if testX1 is not None:
            tempX1, tempX2 = test_data_reshape(testX1, testX2, args, hypers)

            if args.cnn_kernel_size != 0:
                predicted_values = model.predict([tempX2[0], tempX1[0].astype(float)])
            else:
                predicted_values = model.predict(tempX1[0].astype(float))
        else:
            print("test_num is zero. no data is written to aws.")
    if predicted_values is not None:
        # Adding an axis for DLPM & Simple to fit the input structure of write_to_sql function. (ONLY for DLPM & SIMPLE)
        # TODO Change entire DLPA repo to fit this output format???
        if args.production_output_flag:
            predicted_values = np.expand_dims(predicted_values, axis=1)
            write_to_sql(predicted_values, indices_df, args)
        if args.test_stock_output_flag:
            predicted_values = np.expand_dims(predicted_values, axis=1)
            write_to_sql(predicted_values, indices_df, args)
    # Write the model data to AWS
    write_to_sql_model_data(args)
    keras.backend.clear_session()
    return {'loss': 1 - args.best_valid_acc, 'status': STATUS_OK}


def model_returns_candles(trainX1, trainX2, trainY, validX1, validX2, validY, args, hypers):
    if args.train_num != 0:
        batch_size = 2 ** int(hypers['batch_size'])
        learning_rate = 10 ** -int(hypers['learning_rate'])
        lookback = int(hypers['lookback'])

        cnn_kernel_size = int(args.cnn_kernel_size)

        num_layers = int(hypers['num_layers'])
        num_nodes = int(hypers['num_nodes'])
        dropout = float(hypers['dropout'])

        args.param_name_1 = 'num_layers'
        args.param_val_1 = num_layers
        args.param_name_2 = 'num_nodes'
        args.param_val_2 = num_nodes
        args.param_name_3 = 'dropout'
        args.param_val_3 = dropout
        args.param_name_4 = None
        args.param_val_4 = None
        args.param_name_5 = None
        args.param_val_5 = None
    else:
        batch_size = 2 ** int(args.batch_size)
        learning_rate = 10 ** -int(args.learning_rate)
        lookback = int(args.lookback)

        cnn_kernel_size = int(args.cnn_kernel_size)

        num_layers = int(args.param_val_1)
        num_nodes = int(args.param_val_2)
        dropout = float(args.param_val_3)

    input_img2 = Input(shape=(lookback, 1))

    # need dummy dim for channel => (lookback dim, days/wk dim, OHLCV dim, dummy dim) =>
    # (lookback dim, days/wk dim, DOWN-sample, # of kernels) =>
    # (lookback dim, DOWN-sample, DOWN-sample, # of kernels)
    # so for 3 dim data, need 4D data with Conv3D
    # 4D data
    # CNN model
    if args.data_period == 0:  # WEEKLY
        if candle_type_candles == 0:
            input_shape = (lookback, 5, 5, 1)  # looback, 5 days/wk, OHLCV, 1
        else:
            input_shape = (lookback, 1, 5, 1)  # looback, 5 days/wk, rets, 1
    else:  # DAILY
        if candle_type_candles == 0:
            input_shape = (lookback, 5, 1, 1)  # looback, 1 day, OHLCV, 1
        else:
            input_shape = (lookback, 1, 1, 1)  # looback, 1 day, rets, 1

    input_img = Input(shape=input_shape)

    # Conv3D on 4D data
    if candle_type_candles == 0:
        if args.data_period == 0:
            x_1 = Conv3D(cnn_kernel_size, (1, 1, 5), strides=(1, 1, 5), padding='valid', name='conv1')(input_img)
            x_1 = LeakyReLU(alpha=0.1)(x_1)

            x_1 = Conv3D(cnn_kernel_size * 2, (1, 5, 1), strides=(1, 5, 1), padding='valid', name='conv2')(x_1)
            x_1 = LeakyReLU(alpha=0.1)(x_1)
        else:
            x_1 = Conv3D(cnn_kernel_size * 2, (1, 5, 1), strides=(1, 5, 1), padding='valid', name='conv2')(input_img)
            x_1 = LeakyReLU(alpha=0.1)(x_1)
    else:
        if args.data_period == 0:
            x_1 = Conv3D(cnn_kernel_size * 2, (5, 1, 1), strides=(5, 1, 1), padding='valid', name='conv2')(input_img)
            x_1 = LeakyReLU(alpha=0.1)(x_1)
        else:
            x_1 = Conv3D(cnn_kernel_size * 2, (1, 1, 1), strides=(1, 1, 1), padding='valid', name='conv2')(input_img)
            x_1 = LeakyReLU(alpha=0.1)(x_1)

    # x_1 = Reshape((x_1.shape[1] * x_1.shape[2] * x_1.shape[3], 1))(x_1)
    # reshape to lookback X # of kernels
    x_1 = Reshape((lookback, cnn_kernel_size * 2))(x_1)

    x_2 = Dense(num_nodes, input_shape=(lookback, 1))(input_img2)
    for i in range(num_layers - 1):
        x_2 = Dense(num_nodes)(x_2)
        x_2 = Dropout(dropout)(x_2)
    # x_2 = Reshape((x_2.shape[1] * x_2.shape[2], 1))(x_2)

    # merge the final outputs of the GRU's
    x = Concatenate(axis=2)([x_1, x_2])

    # x = tf.keras.backend.expand_dims(x, axis=-1)
    # x = tf.image.resize(x, (args.num_periods_to_predict, int(x.shape[1] * x.shape[2] / args.num_periods_to_predict)))
    # x = tf.squeeze(x, axis=3)
    # # x = Conv2D(args.num_periods_to_predict,args.num_bins)(x)
    # # x = Flatten()(x)
    # # target_dim = (args.num_periods_to_predict*args.num_bins)

    x = Flatten()(x)
    # x = Dense(args.num_bins * args.num_periods_to_predict)(x)
    # x = tf.reshape(x, (-1, args.num_periods_to_predict, args.num_bins))
    x = Dense(args.num_bins, activation='softmax')(x)
    # x = Reshape((args.num_periods_to_predict, args.num_bins))(x)

    adam = tf.keras.optimizers.Adam(lr=learning_rate)
    model = Model([input_img, input_img2], x)
    model.compile(adam, 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])

    model.summary()

    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

    # Checkpoint for saving the model.
    checkpoint = callbacks.ModelCheckpoint(args.model_path + args.model_filename,
                                           monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                           save_best_only=True,
                                           save_weights_only=True, period=1)
    callbacks_list = [checkpoint, es]

    if args.train_num != 0:
        if validX1 is not None:
            history = model.fit(
                [trainX2, trainX1], trainY,
                batch_size=batch_size,
                epochs=args.epoch, validation_data=([validX2, validX1], validY),
                callbacks=callbacks_list)
        else:
            history = model.fit(
                [trainX2, trainX1], trainY,
                batch_size=batch_size,
                epochs=args.epoch, validation_split=.2,
                callbacks=callbacks_list)
    else:
        history = None

    return model, history


def model_returns(trainX1, trainY, validX1, validY, args, hypers):
    if args.train_num != 0:
        batch_size = 2 ** int(hypers['batch_size'])
        learning_rate = 10 ** -int(hypers['learning_rate'])
        lookback = int(hypers['lookback'])

        num_layers = int(hypers['num_layers'])
        num_nodes = int(hypers['num_nodes'])
        dropout = float(hypers['dropout'])

        args.param_name_1 = 'num_layers'
        args.param_val_1 = num_layers
        args.param_name_2 = 'num_nodes'
        args.param_val_2 = num_nodes
        args.param_name_3 = 'dropout'
        args.param_val_3 = dropout
        args.param_name_4 = None
        args.param_val_4 = None
        args.param_name_5 = None
        args.param_val_5 = None
    else:
        batch_size = 2 ** int(args.batch_size)
        learning_rate = 10 ** -int(args.learning_rate)
        lookback = int(args.lookback)

        num_layers = int(args.param_val_1)
        num_nodes = int(args.param_val_2)
        dropout = float(args.param_val_3)

    input_img2 = Input(shape=(lookback, 1))

    x_2 = Dense(args.num_nodes, input_shape=(lookback, 1))(input_img2)
    for i in range(num_layers - 1):
        x_2 = Dense(num_nodes)(x_2)
        x_2 = Dropout(dropout)(x_2)


    x = Flatten()(x_2)
    # x = Dense(args.num_bins * args.num_periods_to_predict)(x)
    #
    # x = tf.reshape(x, (-1, args.num_periods_to_predict, args.num_bins))

    x = Dense(args.num_bins, activation='softmax')(x)

    adam = tf.keras.optimizers.Adam(lr=learning_rate)
    model = Model(input_img2, x)
    model.compile(adam, 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])

    model.summary()

    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

    # Checkpoint for saving the model.
    checkpoint = callbacks.ModelCheckpoint(args.model_path + args.model_filename,
                                           monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                           save_best_only=True,
                                           save_weights_only=True, period=1)
    callbacks_list = [checkpoint, es]

    if args.train_num != 0:
        if validX1 is not None:
            history = model.fit(
                trainX1, trainY,
                batch_size=batch_size,
                epochs=args.epoch, validation_data=(validX1, validY),
                callbacks=callbacks_list)
        else:
            history = model.fit(
                trainX1, trainY,
                batch_size=batch_size,
                epochs=args.epoch, validation_split=.2,
                callbacks=callbacks_list)
    else:
        history = None

    return model, history
