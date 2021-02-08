import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from hyperopt import STATUS_OK
from keras.callbacks import EarlyStopping
from tensorflow.python.keras import callbacks
from tensorflow.python.keras.layers import Input, GRU, Dense, Concatenate, Bidirectional, Attention, Embedding
from tensorflow.python.keras.models import Model

from data.data_output import write_to_sql, write_to_sql_model_data


def full_model(args, trainX, trainY, validX, validY, testX, testY, final_prediction):
    # The attention based sequence to sequence model for weekly returns.
    lookback = args.lookback
    hidden_size = args.hidden_size
    num_bins = args.num_bins
    num_weeks_to_predict = args.num_weeks_to_predict
    batch_size = args.batch_size

    # Since the next sequence is predicted "one by one" in a loop, we should have an input of size 1 for the first
    # iteration.
    decoder_input_df_no_force = np.zeros((trainY.shape[0], 1))
    decoder_input_valid_no_force = np.zeros((validY.shape[0], 1))

    # ******************************************************************************
    # ******************************************************************************
    if not args.embedding_flag:
        trainX = trainX.reshape(-1, trainX.shape[1], 1)
        validX = validX.reshape(-1, validX.shape[1], 1)

    # used for encoder
    encoder_gru = Bidirectional(
        GRU(hidden_size, return_sequences=True, return_state=True, name='encoder_gru', dropout=args.dropout),
        name='bidirectional_encoder')

    # Encoder
    if args.embedding_flag:
        # In case we wanted to use embedding.
        encoder_inputs = Input(shape=(lookback))
        x = Embedding(input_dim=args.unique_num_of_returns, output_dim=hidden_size, input_length=lookback)(
            encoder_inputs)
        encoder_out, encoder_fwd_state, encoder_back_state = encoder_gru(x)
    else:
        encoder_inputs = Input(shape=(lookback, 1))
        encoder_out, encoder_fwd_state, encoder_back_state = encoder_gru(encoder_inputs)

    # ******************************************************************************
    # ******************************************************************************
    # Concatenating the forward and backward encoder states for decoder input.
    decoder_initial_state = Concatenate(axis=-1)([encoder_fwd_state, encoder_back_state])

    # Since the decoder is filled up one by one the input should be one.
    decoder_inputs = Input(shape=1)

    # used for decoder
    decoder_gru = GRU(hidden_size * 2, return_sequences=True, return_state=True, name='decoder_gru',
                      dropout=args.dropout)

    # used for attention
    attn_layer = Attention(args.attention_hidden_size)

    # The final softmax layer for inference.
    dense = Dense(num_bins, activation='softmax', name='softmax_layer')

    all_outputs = []
    inputs = decoder_inputs
    for _ in range(num_weeks_to_predict):
        y = Embedding(input_dim=args.unique_num_of_outputs, output_dim=hidden_size,
                      input_length=num_weeks_to_predict + 1)(inputs)
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

    decoder_outputs = tf.reshape(all_outputs, (-1, num_weeks_to_predict, num_bins))

    optimizer = tf.keras.optimizers.Adam(lr=args.learning_rate)

    full_model = Model(inputs=[encoder_inputs, decoder_inputs], outputs=decoder_outputs)

    full_model.compile(optimizer=optimizer, loss='sparse_categorical_crossentropy',
                       metrics=['sparse_categorical_accuracy'])

    full_model.summary()

    # Early stopping for the model
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)

    # Checkpoint for saving the model.
    checkpoint = callbacks.ModelCheckpoint(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5",
                                           monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                           save_best_only=True,
                                           save_weights_only=True, period=1)
    callbacks_list = [checkpoint, es]

    history = full_model.fit([trainX, decoder_input_df_no_force], trainY, batch_size=batch_size,
                             callbacks=callbacks_list,
                             epochs=args.epoch, validation_data=([validX, decoder_input_valid_no_force], validY),
                             verbose=1)

    # ******************************************************************************
    # ************************* Output to AWS **************************************

    # Predicting uo to 5 weeks for test dataset to report to AWS separately.
    predict_zeros = np.zeros((testX.shape[1], 1))
    scores = np.zeros(5)
    i = 1
    for i in range(5):
        tempX = testX[i, :, :]
        if not args.embedding_flag:
            tempX = tempX.reshape(-1, tempX.shape[1], 1)
        tempY = testY[i, :, :]
        scores[i] = full_model.evaluate([tempX, predict_zeros], tempY)[1]

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

    full_model.load_weights(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5")

    if args.test_output_flag:
        # Write the test data for each stock to AWS
        write_to_sql(testX, None, testY, full_model, args.test_table_name, args)

    if args.production_output_flag:
        # Write the production data for each stock to AWS
        write_to_sql(final_prediction, None, None, full_model, args.production_table_name, args)

    # Write the model data to AWS
    write_to_sql_model_data(args)

    return {'loss': (1 - args.best_valid_acc), 'status': STATUS_OK}
