/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, {Component, PropTypes} from 'react';
import NamespaceStore from 'services/NamespaceStore';
import {MyDatasetApi} from 'api/dataset';
import {MyStreamApi} from 'api/stream';
import FastActionButton from '../FastActionButton';
import ConfirmationModal from 'components/ConfirmationModal';
import T from 'i18n-react';

export default class TruncateAction extends Component {
  constructor(props) {
    super(props);

    this.action = this.action.bind(this);
    this.toggleModal = this.toggleModal.bind(this);

    this.state = {
      modal: false,
      loading: false,
      errorMessage: '',
      extendedMessage: ''
    };
  }

  toggleModal(event) {
    this.setState({modal: !this.state.modal});
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();
  }

  action() {
    this.setState({loading: true});
    let api;
    let params = {
      namespace: NamespaceStore.getState().selectedNamespace
    };
    switch (this.props.entity.type) {
      case 'datasetinstance':
        api = MyDatasetApi.truncate;
        params.datasetId = this.props.entity.id;
        break;
      case 'stream':
        api = MyStreamApi.truncate;
        params.streamId = this.props.entity.id;
        break;
    }

    api(params)
      .subscribe(this.props.onSuccess, (err) => {
        this.setState({
          loading: false,
          errorMessage: T.translate('features.FastAction.truncateFailed', {entityId: this.props.entity.id}),
          extendedMessage: err
        });
      });
  }

  render() {
    const actionLabel = T.translate('features.FastAction.truncateLabel');
    const headerTitle = `${actionLabel} ${this.props.entity.type}`;

    return (
      <span>
        <FastActionButton
          icon="fa fa-scissors"
          action={this.toggleModal}
        />
        {
          this.state.modal ? (
            <ConfirmationModal
              headerTitle={headerTitle}
              toggleModal={this.toggleModal}
              confirmationText={T.translate('features.FastAction.truncateConfirmation', {entityId: this.props.entity.id})}
              confirmButtonText={actionLabel}
              confirmFn={this.action}
              cancelFn={this.toggleModal}
              isOpen={this.state.modal}
              isLoading={this.state.loading}
              errorMessage={this.state.errorMessage}
              extendedMessage={this.state.extendedMessage}
            />
          ) : null
        }
      </span>
    );
  }
}

TruncateAction.propTypes = {
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    type: PropTypes.oneOf(['datasetinstance', 'stream']).isRequired,
  }),
  onSuccess: PropTypes.func
};
