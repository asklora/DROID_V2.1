import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf
from keras.callbacks import EarlyStopping
from tensorflow.python.keras import callbacks
from tensorflow.python.keras.layers import Input, GRU, Dense, Concatenate, Bidirectional, Attention, \
    Flatten, Embedding
from tensorflow.python.keras.models import Model

from data.data_output import write_to_sql, write_to_sql_model_data


def full_model(trainX, trainY, validX, validY, testX, testY, final_prediction, args):
    lookback = args.lookback
    hidden_size = args.hidden_size
    attention_hidden_size = args.attention_hidden_size
    num_bins = args.num_bins
    num_weeks_to_predict = args.num_weeks_to_predict
    batch_size = args.batch_size

    decoder_input_df_no_force = np.zeros((trainY.shape[0], 1))
    decoder_input_valid_no_force = np.zeros((validY.shape[0], 1))
    if testX is not None:
        decoder_input_test_no_force = np.zeros((testY.shape[0], 1))

    # mirrored_strategy = tf.distribute.MirroredStrategy(devices=["/gpu:0", "/gpu:1"])
    # mirrored_strategy = tf.distribute.MirroredStrategy(devices=["/gpu:0"])
    # with mirrored_strategy.scope():
    # ******************************************************************************
    # ******************************************************************************
    # Encoder
    encoder_inputs = Input(shape=(lookback))
    x = Embedding(input_dim=args.unique_num_of_returns, output_dim=hidden_size, input_length=lookback)(encoder_inputs)
    encoder_gru = Bidirectional(
        GRU(hidden_size, return_sequences=True, return_state=True, name='encoder_gru', dropout=args.dropout),
        name='bidirectional_encoder')

    encoder_out, encoder_fwd_state, encoder_back_state = encoder_gru(x)
    # encoder_states = [encoder_fwd_state, encoder_back_state]
    # ******************************************************************************
    # ******************************************************************************
    # Decoder
    decoder_initial_state = Concatenate(axis=-1)([encoder_fwd_state, encoder_back_state])
    decoder_inputs = Input(shape=1)
    # y = Embedding(input_dim=unique_num_of_outputs, output_dim=hidden_size, input_length=num_weeks_to_predict + 1)(
    #     decoder_inputs)

    decoder_gru = GRU(hidden_size * 2, return_sequences=True, return_state=True, name='decoder_gru',
                      dropout=args.dropout)

    # decoder_out, decoder_state = decoder_gru(y, initial_state=decoder_initial_state)

    # Attention layer
    attn_layer = Attention(attention_hidden_size)
    # query_value_attention_seq = attn_layer([encoder_out, decoder_out])
    # decoder_concat_input = Concatenate(axis=1, name='concat_layer')([decoder_out, query_value_attention_seq])
    # decoder_concat_input2 = tf.reshape(decoder_concat_input,
    #                                    (-1, num_weeks_to_predict + 1, int(
    #                                        (decoder_concat_input.shape[1] * decoder_concat_input.shape[2]) / (
    #                                                num_weeks_to_predict + 1))))

    dense = Dense(num_bins, activation='softmax', name='softmax_layer')
    # decoder_pred = dense(decoder_concat_input2)
    flatt = Flatten()

    all_outputs = []
    inputs = decoder_inputs
    for _ in range(num_weeks_to_predict):
        y = Embedding(input_dim=args.unique_num_of_outputs, output_dim=hidden_size,
                      input_length=num_weeks_to_predict + 1)(inputs)
        decoder_out, decoder_state = decoder_gru(y, initial_state=decoder_initial_state)

        # Attention layer
        query_value_attention_seq = attn_layer([encoder_out, decoder_out])
        decoder_concat_input = Concatenate(axis=1)([decoder_out, query_value_attention_seq])
        decoder_concat_input2 = tf.reshape(decoder_concat_input,
                                           (-1, 1, decoder_concat_input.shape[1] * decoder_concat_input.shape[2]))
        # decoder_concat_input2 = flatt(decoder_concat_input)
        # dense = Dense(num_bins + 2, activation='softmax', name='softmax_layer')
        outputs = dense(decoder_concat_input2)
        all_outputs.append(outputs)
        # outputs = decoder_pred(decoder_pred)
        inputs = tf.math.argmax(outputs, axis=2)
        decoder_initial_state = decoder_state
    all_outputs = tf.stack(all_outputs)
    decoder_outputs = tf.reshape(all_outputs, (-1, num_weeks_to_predict, num_bins))
    # decoder_outputs = Lambda(lambda x: K.concatenate(x, axis=1))(all_outputs)

    # Full model

    optimizer = tf.keras.optimizers.Adam(lr=args.learning_rate)

    full_model = Model(inputs=[encoder_inputs, decoder_inputs], outputs=decoder_outputs)

    full_model.compile(optimizer=optimizer, loss='sparse_categorical_crossentropy',
                       metrics=['sparse_categorical_accuracy'])
    # full_model.compile(optimizer=optimizer, loss='categorical_crossentropy', metrics=['accuracy'])

    full_model.summary()
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=30)
    checkpoint = callbacks.ModelCheckpoint(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5",
                                           monitor='val_sparse_categorical_accuracy', mode='max', verbose=1,
                                           save_best_only=True,
                                           save_weights_only=True, period=1)
    callbacks_list = [checkpoint, es]
    # callbacks_list = [checkpoint]
    # full_model.fit([trainX[:1000], decoder_input_df_no_force[:1000]], trainY[:1000], batch_size=batch_size,
    # callbacks=callbacks_list, epochs=10,validation_data=([validX[:1000], decoder_input_valid_no_force[:1000]],
    # validY[:1000]),verbose=1)
    history = full_model.fit([trainX, decoder_input_df_no_force], trainY, batch_size=batch_size,
                             callbacks=callbacks_list,
                             epochs=args.epoch, validation_data=([validX, decoder_input_valid_no_force], validY),
                             verbose=1)
    # full_model.save_weights('s2s.h5')

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
        plt.figure()
        plt.plot(history.history['sparse_categorical_accuracy'])
        plt.plot(history.history['val_sparse_categorical_accuracy'])
        plt.title('model accuracy')
        plt.ylabel('accuracy')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        # plt.show()
        plt.savefig(args.plot_path + "plot_acc_" + str(int(args.timestamp)) + ".png")
        # summarize history for loss
        plt.figure()
        plt.plot(history.history['loss'])
        plt.plot(history.history['val_loss'])
        plt.title('model loss')
        plt.ylabel('loss')
        plt.xlabel('epoch')
        plt.legend(['train', 'test'], loc='upper left')
        # plt.show()
        plt.savefig(args.plot_path + "plot_loss_" + str(int(args.timestamp)) + ".png")

    full_model.load_weights(args.model_path + "model_" + str(int(args.timestamp)) + ".hdf5")
    # df = testX

    if args.test_output_flag:
        write_to_sql(testX, None, testY, full_model, args.test_table_name, args)

    if args.production_output_flag:
        write_to_sql(final_prediction, None, None, full_model, args.production_table_name, args)

    write_to_sql_model_data(args)
