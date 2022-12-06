const Worker = function (director, {id, workFunction, onWorkError, params, methods}) {
    this.director = director;
    this.queue = [];
    this.id = id;
    this.isBusy = false;
    this.isTemp = false;

    this.fillQueue = function (items) {
        this.queue = items;
    };

    if (!workFunction)throw new Error('workFunction is required');
    if (typeof workFunction !== 'function' || workFunction.constructor.name !== "AsyncFunction")
        throw new Error('workFunction must be an async function');

    if (typeof onWorkError !== 'function') {
        onWorkError = () => {
        };
    }

    this.work = async function () {
        let result = [];
        this.isBusy = true;
        do {
            const promises = [];

            for (let i = 0; i < this.queue.length; i++) {
                const item = this.queue.shift();
                promises.push(
                    workFunction(this, item).catch((error) => {
                        if (this.director.detailLog) this.director.logger.error(error);
                        onWorkError(this, item, error);
                    })
                );
            }

            const results = await Promise.all(promises);
            result.push(...results);
        } while (this.queue.length > 0);
        this.isBusy = false;

        return result;
    };

    if (params) {
        for (let paramKey in params) {
            if (params.hasOwnProperty(paramKey)) {
                if (this.hasOwnProperty(paramKey))throw new Error(`Param name ${paramKey} is already in use`);
                this[paramKey] = params[paramKey];
            }
        }
    }

    if (methods) {
        for (let methodKey in methods) {
            if (methods.hasOwnProperty(methodKey)) {
                if (this.hasOwnProperty(methodKey))throw new Error(`Method name ${methodKey} is already in use`);
                this[methodKey] = methods[methodKey];
            }
        }
    }


    if (this.director.detailLog) this.director.logger.info('Created worker with id ' + this.id);
};


module.exports = Worker;