import keras.backend
import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from hyperopt import STATUS_OK
from keras.callbacks import EarlyStopping
from tensorflow.python.keras import callbacks
from tensorflow.python.keras.layers import (
    Input, GRU, Dense, Concatenate, 
    Bidirectional, Attention, 
    Flatten, Embedding, LeakyReLU, 
    Conv3D, Reshape, Dropout)
from tensorflow.python.keras.models import Model

from dlpa.data.data_output import write_to_sql, write_to_sql_model_data
from dlpa.data.data_preprocess import test_data_reshape
from dlpa.model.ec2_fns import save_to_ec2, load_from_ec2
from dlpa.global_vars import epoch, num_bins, model_path

def full_model(trainX1, trainX2, trainY, validX1, validX2, validY, testX1, testX2, testY, final_prediction1,
               final_prediction2, indices_df, hypers, simple=False, dlpa=False, dlpm=False):
    if args.test_num != 0:
        # Removing the nan rows in test datasets
        args.test_mask = np.squeeze(args.test_mask)
        testX1 = testX1[:, args.test_mask, :]
        if testX2 is not None:
            testX2 = testX2[:, args.test_mask, :, :, :, :]
        testY = testY[:, args.test_mask, :]

    if args.cnn_kernel_size != 0:
        model, history = model_returns_candles(trainX1, trainX2, trainY, validX1, validX2, validY, hypers, simple=simple, dlpa=dlpa, dlpm=dlpm)
    else:
        model, history = model_returns(trainX1, trainY, validX1, validY, hypers, simple=simple, dlpa=dlpa, dlpm=dlpm)

    # train_num = 0 means going to inference mode.
    if args.train_num != 0:
        if args.save_ec2:
            save_to_ec2(args)

    if args.train_num != 0:
        model.load_weights(model_path + args.model_filename)
    else:
        load_from_ec2(args)
        model.load_weights(model_path + args.model_filename)
    # ******************************************************************************
    # ************************* Output to AWS **************************************

    # Predicting uo to 5 weeks for test dataset to report to AWS separately.
    if testX1 is not None:
        if dlpa :
            predict_zeros = np.zeros((testX1.shape[1], 1))

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
            if dlpm or simple:
                predicted_values = np.expand_dims(predicted_values, axis=1)
            write_to_sql(predicted_values, indices_df, args)
        if args.test_stock_output_flag:
            if dlpm or simple:
                predicted_values = np.expand_dims(predicted_values, axis=1)
            write_to_sql(predicted_values, indices_df, args)
    # Write the model data to AWS
    write_to_sql_model_data(args)
    keras.backend.clear_session()
    return {'loss': 1 - args.best_valid_acc, 'status': STATUS_OK}

def model_returns_candles(trainX1, trainX2, trainY, validX1, validX2, validY, hypers, simple=False, dlpa=False, dlpm=False):
    if args.train_num != 0:
        batch_size = 2 ** int(hypers['batch_size'])
        learning_rate = 10 ** -int(hypers['learning_rate'])
        lookback = int(hypers['lookback'])

        # we need cnn size
        cnn_kernel_size = int(args.cnn_kernel_size)

        if(dlpm):
            # hypers from hyper space
            g_mult = float(hypers['gru_nodes_mult'])
            wr_g_mult = int(hypers['wr_gru_nodes_mult'])
            gru_drop = float(hypers['gru_drop'])
            wr_gru_drop = float(hypers['wr_gru_drop'])
            gru_nodes = int(hypers['gru_nodes'])  # of parallel GRU nodes for candle inputs
            wr_gru_nodes = int(hypers['wr_gru_nodes'])  # of parallel GRU nodes for weekly return inputs

            args.param_name_1 = 'gru_nodes_mult'
            args.param_val_1 = g_mult
            args.param_name_2 = 'wr_gru_nodes_mult'
            args.param_val_2 = wr_g_mult
            args.param_name_3 = 'gru_drop'
            args.param_val_3 = gru_drop
            args.param_name_4 = 'wr_gru_drop'
            args.param_val_4 = wr_gru_drop
            args.param_name_5 = 'gru_nodes'
            args.param_val_5 = gru_nodes

        if (simple):
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

        if (dlpa):
            hidden_size = int(hypers['num_hidden'])
            attention_hidden_size = int(hypers['num_hidden_att'])
            dropout = float(hypers['dropout'])

            args.param_name_1 = 'num_hidden'
            args.param_val_1 = hidden_size
            args.param_name_2 = 'num_hidden_att'
            args.param_val_2 = attention_hidden_size
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

        if (dlpm):
            g_mult = float(args.param_val_1)
            wr_g_mult = int(args.param_val_2)
            gru_drop = float(args.param_val_3)
            wr_gru_drop = float(args.param_val_4)
            gru_nodes = int(args.param_val_5)

        if (simple):
            num_layers = int(args.param_val_1)
            num_nodes = int(args.param_val_2)
            dropout = float(args.param_val_3)

        if (dlpa):
            hidden_size = int(args.param_val_1)
            attention_hidden_size = int(args.param_val_2)
            dropout = float(args.param_val_3)

    # expected input data shape: (batch_size, lookback, data_dim)
    if(dlpm):
        g_n = 2  # #of GRU layers
        wr_g_n = 2  # #of weekly ret GRU layers

    if(dlpa):
        num_periods_to_predict = args.num_periods_to_predict
        # Since the next sequence is predicted "one by one" in a loop, we should have an input of size 1 for the first
        # iteration.
        decoder_input_df_no_force = np.zeros((trainY.shape[0], 1))
        if validX1 is not None:
            decoder_input_valid_no_force = np.zeros((validY.shape[0], 1))

        # used for encoder
        encoder_gru = Bidirectional(
            GRU(hidden_size, return_sequences=True, return_state=True, name='encoder_gru', dropout=dropout),
            name='bidirectional_encoder')

    # ---------------CNN PART-------------------------------------------------------------------------------
    # need dummy dim for channel => (lookback dim, days/wk dim, OHLCV dim, dummy dim) =>
    # (lookback dim, days/wk dim, DOWN-sample, # of kernels) =>
    # (lookback dim, DOWN-sample, DOWN-sample, # of kernels)
    # so for 3 dim data, need 4D data with Conv3D
    # 4D data

    if args.data_period == 0:  # WEEKLY
        if args.candle_type_candles == 0:  # RECOMMEND
            input_shape = (lookback, 5, 5, 1)  # lookback, 5 days/wk, OHLCV, 1
        else:
            input_shape = (lookback, 1, 5, 1)  # lookback, 5 days/wk, rets, 1
    else:  # DAILY
        if args.candle_type_candles == 0:  # RECOMMEND
            input_shape = (lookback, 5, 1, 1)  # lookback, 1 day, OHLCV, 1
        else:
            input_shape = (lookback, 1, 1, 1)  # lookback, 1 day, rets, 1
    
    if(dlpm):
        print('DLPM NOT RECOMMENDED WITHOUT OHLCV CANDLES!!!!!!!!')  # DON'T RECOMMEND DLPM without candles
    
    input_img = Input(shape=input_shape)
    
    if(dlpm or simple):
        input_img2 = Input(shape=(lookback, 1))

        # Conv3D on 4D data
    if args.candle_type_candles == 0:
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

    if(dlpm):
        # x_1 = Reshape((x_1.shape[1] * x_1.shape[2] * x_1.shape[3], 1))(x_1)
        # reshape to lookback X # of kernels
        x_1 = Reshape((lookback, cnn_kernel_size * 2))(x_1)

            # ---------------GRU PART-------------------------------------------------------------------------------
        for i in range(wr_g_n):
            extra = dict(return_sequences=True)
            temp_nodes2 = min(wr_gru_nodes * (2 ** (wr_g_mult * (i))), 8)
            if i == 0:
                # extra.update(input_shape=(input_shape2))
                x_2 = GRU(temp_nodes2, **extra)(input_img2)
            elif i == wr_g_n - 1:
                extra = dict(return_sequences=False)  # last layer does not output the whole sequence
                x_2_2 = GRU(temp_nodes2, **extra)(x_2)
                extra = dict(return_sequences=True)
                x_2 = GRU(1, dropout=0, **extra)(x_2)
            else:
                x_2 = GRU(temp_nodes2, dropout=wr_gru_drop, **extra)(x_2)
        x_2 = Flatten()(x_2)

        for i in range(g_n):
            extra = dict(return_sequences=True)
            temp_nodes = int(min(gru_nodes * (2 ** (g_mult * i)), 8))
            if i == 0:
                # extra.update(input_shape=(lookback, number_of_kernels * 2))
                x_1 = GRU(temp_nodes, **extra)(x_1)
            elif i == g_n - 1:
                extra = dict(return_sequences=False)  # last layer does not output the whole sequence
                x_1_2 = GRU(temp_nodes, **extra)(x_1)
                extra = dict(return_sequences=True)
                x_1 = GRU(1, dropout=0, **extra)(x_1)
            else:
                x_1 = GRU(temp_nodes, dropout=gru_drop, **extra)(x_1)
        x_1 = Flatten()(x_1)

            # merge the final outputs of the GRU's and TWO dense layers----------------------------------------------
        x = Concatenate(axis=1)([x_1, x_1_2, x_2, x_2_2])

        x = Dense(lookback + 1)(x)
        x = Dense(lookback + 1)(x)
        if args.num_periods_to_predict > 1:
            print('ERROR NEED args.num_periods_to_predict = 1!!!!!!!!!!!!!!!!!!')

        x = Flatten()(x)
        # x = Dense(args.num_bins * args.num_periods_to_predict)(x)
        # x = tf.reshape(x, (-1, args.num_periods_to_predict, args.num_bins))
        x = Dense(num_bins, activation='softmax')(x)
        adam = tf.keras.optimizers.Adam(lr=learning_rate)
        model = Model([input_img, input_img2], x)
        model.compile(adam, 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])
        model.summary()

        es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

        # Checkpoint for saving the model.
        checkpoint = callbacks.ModelCheckpoint(model_path + args.model_filename,
                                            monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                            save_best_only=True,
                                            save_weights_only=True, period=1)
        callbacks_list = [checkpoint, es]

        if args.train_num != 0:
            if validX1 is not None:
                history = model.fit(
                    [trainX2, trainX1], trainY,
                    validation_data=([validX2, validX1], validY),
                    batch_size=batch_size,
                    epochs=epoch,
                    callbacks=callbacks_list)
            else:
                history = model.fit(
                    [trainX2, trainX1], trainY,
                    validation_split=.2,
                    batch_size=batch_size,
                    epochs=epoch,
                    callbacks=callbacks_list)
        else:
            history = None
        return model, history
        
    if(dlpa):
        flatt = Flatten()
        x_1 = flatt(x_1)
        CNN_dense = Dense(lookback, activation='relu')
        x_1 = CNN_dense(x_1)
        x_1 = tf.reshape(x_1, (-1, x_1.shape[1], 1))

        # Encoder
        if args.embedding_flag:
            # In case we wanted to use embedding.
            encoder_inputs = Input(shape=(lookback), name='enc_input')
            x = Embedding(input_dim=args.unique_num_of_returns, output_dim=hidden_size, input_length=2 * lookback)(
                encoder_inputs)

            # Concatenating CNN output and encoder input
            encoder_inputs2 = Concatenate(axis=2)([x_1, x])

            encoder_out, encoder_fwd_state, encoder_back_state = encoder_gru(encoder_inputs2)

        else:
            encoder_inputs = Input(shape=(lookback, 1))

            # Concatenating CNN output and encoder input
            encoder_inputs2 = Concatenate(axis=2)([x_1, encoder_inputs])

            encoder_out, encoder_fwd_state, encoder_back_state = encoder_gru(encoder_inputs2)

        # ******************************************************************************
        # ******************************************************************************
        # Concatenating the forward and backward encoder states for decoder input.
        decoder_initial_state = Concatenate(axis=-1)([encoder_fwd_state, encoder_back_state])

        # Since the decoder is filled up one by one the input should be one.
        decoder_inputs = Input(shape=1, name='dec_input')

        # used for decoder
        decoder_gru = GRU(hidden_size * 2, return_sequences=True, return_state=True, name='decoder_gru',
                        dropout=dropout)

        # used for attention
        attn_layer = Attention(attention_hidden_size)

        # The final softmax layer for inference.
        dense = Dense(num_bins, activation='softmax', name='softmax_layer')

        all_outputs = []
        inputs = decoder_inputs
        for _ in range(num_periods_to_predict):
            y = Embedding(input_dim=args.unique_num_of_outputs, output_dim=hidden_size,
                        input_length=num_periods_to_predict + 1)(inputs)
            decoder_out, decoder_state = decoder_gru(y, initial_state=decoder_initial_state)

            # Attention layer
            query_value_attention_seq = attn_layer([encoder_out, decoder_out])

            # Concatenating the attention and decoder outputs.
            decoder_concat_input = Concatenate(axis=1)([decoder_out, query_value_attention_seq])

            decoder_concat_input2 = tf.reshape(decoder_concat_input,
                                            (-1, 1, decoder_concat_input.shape[1] * decoder_concat_input.shape[2]))

            outputs = dense(decoder_concat_input2)

            # At each iteration one part of the second sequence is predicted and then appended to previous outputs.
            all_outputs.append(outputs)

            # Predicting the output class of predicted values in each iteration
            inputs = tf.math.argmax(outputs, axis=2)

            # Use the last decoder state for the initial state of decoder layer for the next iteration
            decoder_initial_state = decoder_state

        all_outputs = tf.stack(all_outputs)
        decoder_outputs = tf.reshape(all_outputs, (-1, num_periods_to_predict, num_bins))

        optimizer = tf.keras.optimizers.Adam(lr=learning_rate)

        model = Model(inputs=[input_img, encoder_inputs, decoder_inputs], outputs=decoder_outputs)

        model.compile(optimizer=optimizer, loss='sparse_categorical_crossentropy',
                        metrics=['sparse_categorical_accuracy'])

        model.summary()

        # Early stopping for the model
        es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

        # Checkpoint for saving the model.
        checkpoint = callbacks.ModelCheckpoint(model_path + args.model_filename,
                                            monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                            save_best_only=True,
                                            save_weights_only=True, period=1)
        callbacks_list = [checkpoint, es]

        if args.train_num != 0:
            if validX1 is not None:
                # USE A VALIDATION SET
                history = model.fit([trainX2, trainX1, decoder_input_df_no_force], trainY, batch_size=batch_size,
                                        callbacks=callbacks_list,
                                        epochs=epoch,
                                        validation_data=([validX2, validX1, decoder_input_valid_no_force], validY),
                                        verbose=1)
            else:
                # USE A RANDOM SPLIT
                history = model.fit([trainX2, trainX1, decoder_input_df_no_force], trainY, batch_size=batch_size,
                                        callbacks=callbacks_list,
                                        epochs=epoch,
                                        validation_split=.2,
                                        verbose=1)
        else:
            history = None

        return model, history

    if(simple):
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
        x = Flatten()(x)
        x = Dense(num_bins, activation='softmax')(x)

        adam = tf.keras.optimizers.Adam(lr=learning_rate)
        model = Model([input_img, input_img2], x)
        model.compile(adam, 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])

        model.summary()

        es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

        # Checkpoint for saving the model.
        checkpoint = callbacks.ModelCheckpoint(model_path + args.model_filename,
                                            monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                            save_best_only=True,
                                            save_weights_only=True, period=1)
        callbacks_list = [checkpoint, es]

        if args.train_num != 0:
            if validX1 is not None:
                history = model.fit(
                    [trainX2, trainX1], trainY,
                    batch_size=batch_size,
                    epochs=epoch, validation_data=([validX2, validX1], validY),
                    callbacks=callbacks_list)
            else:
                history = model.fit(
                    [trainX2, trainX1], trainY,
                    batch_size=batch_size,
                    epochs=epoch, validation_split=.2,
                    callbacks=callbacks_list)
        else:
            history = None

        return model, history






